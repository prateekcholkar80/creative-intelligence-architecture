"""4.4 Critical/Must have/shared pilot: opt-in ASGI sharing boundary.

Trusted composition supplies a prestarted host. No launcher, automatic restart,
identity fallback, generation dispatch or durable audit writer is installed here.
"""
from __future__ import annotations

import asyncio
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from dataclasses import dataclass
import json
import re
from urllib.parse import urlsplit
from uuid import uuid4

from src.application.identity import ASSERTION_HEADER
from src.application.operation_budget import CancellationSignal, OperationStopped, operation_scope
from src.application.operation_channel import ChannelError, MAX_FRAME_BYTES, encode
from src.application.sharing_host_contract import METHODS, pack_result, request_values
from src.application.sharing_operation_host import SharingHostError, SharingOperationHost


@dataclass(frozen=True, slots=True)
class SharingApiSettings:
    enabled: bool = False
    allowed_origins: tuple[str, ...] = ()
    max_active_requests: int = 4

    def __post_init__(self):
        if (type(self.enabled) is not bool or type(self.allowed_origins) is not tuple
                or len(self.allowed_origins) > 16
                or any(type(origin) is not str for origin in self.allowed_origins)
                or len(set(self.allowed_origins)) != len(self.allowed_origins)
                or type(self.max_active_requests) is not int
                or not 1 <= self.max_active_requests <= 32):
            raise ValueError('Invalid trusted sharing API settings')
        for origin in self.allowed_origins:
            if type(origin) is not str:
                raise ValueError('An exact HTTPS origin is required')
            try:
                parsed = urlsplit(origin)
                valid = (parsed.scheme == 'https' and parsed.hostname is not None
                    and parsed.username is None and parsed.password is None
                    and not parsed.path and not parsed.query and not parsed.fragment
                    and parsed.port != 0 and len(origin) <= 256
                    and '*' not in origin and origin.isascii()
                    and not any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in origin))
            except ValueError:
                valid = False
            if not valid:
                raise ValueError('An exact HTTPS origin is required')
        if self.enabled and not self.allowed_origins:
            raise ValueError('Enabled sharing API requires trusted origins')


@dataclass(frozen=True, slots=True)
class ApiDeliveryObservation:
    """Bounded local diagnostic, never an access decision or durable audit row.

    worker_requested means the host was called, not that SQL was submitted.
    response_completed means ASGI accepted the final body, not client receipt.
    Neither field proves commit, rollback or absence of charges.
    """
    request_id: str
    operation: str | None
    worker_requested: bool
    response_completed: bool
    status: int | None
    code: str


class _ApiError(Exception):
    def __init__(self, status, code):
        self.status, self.code = status, code


class _Disconnected(Exception):
    pass


def _unique(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ChannelError()
        value[key] = item
    return value


def _parse_body(raw):
    try:
        value = json.loads(raw.decode('utf-8'), object_pairs_hook=_unique,
            parse_constant=lambda _: (_ for _ in ()).throw(ChannelError()))
        if type(value) is not dict:
            raise ChannelError()
        return value
    except (ValueError, TypeError, RecursionError, ChannelError):
        raise _ApiError(400, 'INVALID_REQUEST') from None


def _request(scope, settings, *, prefix='/v1/sharing/', methods=METHODS):
    if scope.get('method') != 'POST':
        raise _ApiError(405, 'METHOD_NOT_ALLOWED')
    method = scope.get('path', '').removeprefix(prefix)
    if scope.get('path') != prefix + method or method not in methods:
        raise _ApiError(404, 'NOT_FOUND')
    if scope.get('query_string', b''):
        raise _ApiError(400, 'INVALID_REQUEST')
    # Do not forward cookies, email, roles, forwarded host or deadline headers.
    headers = {}
    raw = scope.get('headers', [])
    if len(raw) > 64:
        raise _ApiError(400, 'INVALID_REQUEST')
    total = 0
    try:
        for name, value in raw:
            if type(name) is not bytes or type(value) is not bytes:
                raise ValueError()
            total += len(name) + len(value)
            if total > 32768 or len(name) > 128 or len(value) > 16384:
                raise ValueError()
            name = name.decode('ascii').lower()
            value = value.decode('ascii')
            if (re.fullmatch(r"[!#$%&'*+.^_`|~0-9a-z-]+",name) is None
                    or any(ord(c) < 32 and c != '\t' or ord(c) == 127 for c in value)):
                raise ValueError()
            if name in ('origin', 'content-type', 'content-length', ASSERTION_HEADER):
                if name in headers:
                    raise ValueError()
                headers[name] = value
    except (ValueError, TypeError, UnicodeError):
        raise _ApiError(400, 'INVALID_REQUEST') from None
    if headers.get('origin') not in settings.allowed_origins or not headers.get(ASSERTION_HEADER):
        raise _ApiError(403, 'ACCESS_DENIED')
    if headers.get('content-type', '').lower() not in ('application/json', 'application/json; charset=utf-8'):
        raise _ApiError(415, 'UNSUPPORTED_MEDIA_TYPE')
    length = headers.get('content-length')
    if length is not None and (not length.isascii() or not length.isdecimal()
            or len(length) > 10 or not 1 <= int(length) <= MAX_FRAME_BYTES):
        raise _ApiError(400, 'INVALID_REQUEST')
    return method, {ASSERTION_HEADER: headers[ASSERTION_HEADER]}, None if length is None else int(length)


async def _join(task):
    """Drain owned tasks even if shutdown repeatedly cancels the request task."""
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
        except Exception:
            break
    # Retrieve errors; cleanup must not replace the safe response/outcome.
    if not task.cancelled():
        task.exception()


class SharingApi:
    """ASGI HTTP callable; mount only through explicitly trusted server setup.

    One event loop owns admission and bounded diagnostics. A thread supervises
    each admitted host call; all cloud/provider work stays in the owned process.
    Every supervisor task is joined before returning, including disconnects.
    No lifecycle prewarm or restart is triggered by HTTP input.
    """
    response_version = 'sharing-api-v1'

    def __init__(self, host: SharingOperationHost, settings: SharingApiSettings = SharingApiSettings()):
        if not self._valid_host(host) or type(settings) is not SharingApiSettings:
            raise ValueError('Typed trusted sharing API composition is required')
        self._host, self._settings = host, settings
        self._active = 0
        self._loop = None
        self._observations = deque(maxlen=256)
        self._pool = ThreadPoolExecutor(max_workers=settings.max_active_requests,
                                        thread_name_prefix='sharing-api-host')
        self._signals = set()
        self._tasks = set()
        self._requests = set()
        self._closed = False
        self._pool_closed = False

    def _valid_host(self, host):
        return type(host) is SharingOperationHost

    def _request(self, scope):
        return _request(scope, self._settings)

    def _validate_parameters(self, operation, headers, parameters):
        request_values(operation, headers, parameters)

    def _invoke_operation(self, operation, headers, parameters, cancellation):
        method = getattr(self._host, operation)
        if operation == 'issue_intent':
            return method(headers, cancellation=cancellation, **parameters)
        return method(headers, parameters['token'], cancellation=cancellation)

    async def _respond_result(self, operation, value, request_id, respond):
        await respond(200, {'version':'sharing-api-v1','request_id':request_id,
            'operation':operation,'result':pack_result(operation,value)})

    @property
    def observations(self) -> tuple[ApiDeliveryObservation, ...]:
        return tuple(self._observations)

    @property
    def active_requests(self) -> int:
        return self._active

    async def aclose(self) -> bool:
        """Explicit server teardown; stop admission, drain, then close owned host."""
        loop = asyncio.get_running_loop()
        if self._loop is not None and self._loop is not loop:
            raise ValueError('Sharing API must be owned by one event loop')
        self._loop = loop
        self._closed = True
        for signal in tuple(self._signals):
            signal.cancel()
        for request in tuple(self._requests):
            await _join(request)
        for task in tuple(self._tasks):
            await _join(task)
        if self._pool_closed:
            return self._host.cleanup_complete
        try:
            task = asyncio.wrap_future(self._pool.submit(self._host.close))
        except RuntimeError:
            return self._host.cleanup_complete
        await _join(task)
        result = task.result()
        self._pool.shutdown(wait=True, cancel_futures=True)
        self._pool_closed = True
        return result

    async def __call__(self, scope, receive, send):
        if scope.get('type') != 'http':
            raise ValueError('Sharing API supports HTTP only; lifecycle is server-owned')
        loop = asyncio.get_running_loop()
        if self._loop is not None and self._loop is not loop:
            raise ValueError('Sharing API must be owned by one event loop')
        self._loop = loop
        request_id = uuid4().hex
        operation = None
        worker_requested = response_completed = False
        status = None
        code = 'REQUEST_FAILED'
        task = watcher = None
        cancellation = CancellationSignal()
        admitted = False
        response_started = False
        try:
            with operation_scope(self._host.operation_settings, cancellation) as budget:
                async def bounded(awaitable):
                    pending = asyncio.ensure_future(awaitable)
                    try:
                        while True:
                            done, _ = await asyncio.wait((pending,), timeout=min(.02,budget.remaining()))
                            if done:
                                budget.remaining()
                                return pending.result()
                    finally:
                        if not pending.done():
                            pending.cancel()
                        await _join(pending)

                async def respond(http_status, payload=None, *, raw=None, headers=()):
                    nonlocal response_started, response_completed, status
                    body = encode(payload)[4:] if raw is None else raw
                    status = http_status
                    # Check once more before starting or releasing the body.
                    budget.remaining()
                    response_started = True
                    await bounded(send({'type':'http.response.start','status':http_status,'headers':[
                        *headers, *([] if headers else [(b'content-type',b'application/json')]), (b'cache-control',b'no-store'),
                        (b'x-content-type-options',b'nosniff'),
                        (b'content-length',str(len(body)).encode('ascii'))]}))
                    budget.remaining()
                    await bounded(send({'type':'http.response.body','body':body,'more_body':False}))
                    response_completed = True

                try:
                    if not self._settings.enabled or self._closed:
                        raise _ApiError(503, 'SERVICE_UNAVAILABLE')
                    operation, headers, expected_length = self._request(scope)
                    if self._active >= self._settings.max_active_requests:
                        raise _ApiError(503, 'SERVICE_BUSY')
                    self._active += 1
                    self._signals.add(cancellation)
                    self._requests.add(asyncio.current_task())
                    admitted = True
                    raw = bytearray()
                    while True:
                        event = await bounded(receive())
                        if event.get('type') == 'http.disconnect':
                            raise _Disconnected()
                        if (event.get('type') != 'http.request' or type(event.get('body', b'')) is not bytes
                                or type(event.get('more_body', False)) is not bool):
                            raise _ApiError(400, 'INVALID_REQUEST')
                        part = event.get('body', b'')
                        if len(raw) + len(part) > MAX_FRAME_BYTES:
                            raise _ApiError(413, 'REQUEST_TOO_LARGE')
                        raw.extend(part)
                        if not event.get('more_body', False):
                            break
                    if expected_length is not None and len(raw) != expected_length:
                        raise _ApiError(400, 'INVALID_REQUEST')
                    parameters = _parse_body(raw)
                    try:
                        self._validate_parameters(operation, headers, parameters)
                    except (ChannelError, TypeError, ValueError):
                        raise _ApiError(400, 'INVALID_REQUEST') from None

                    async def watch_disconnect():
                        # Body is fully consumed. Any further request message is
                        # a protocol failure; no second command can be smuggled.
                        try:
                            await receive()
                        finally:
                            cancellation.cancel()
                        return None

                    watcher = asyncio.create_task(watch_disconnect())
                    def invoke():
                        from src.application.access_capture import delivery_trace
                        with delivery_trace(request_id):
                            return self._invoke_operation(operation, headers, parameters, cancellation)
                    worker_requested = True
                    context = copy_context()
                    task = asyncio.wrap_future(self._pool.submit(context.run, invoke))
                    self._tasks.add(task)
                    done, _ = await asyncio.wait((task,watcher),timeout=budget.remaining(),
                                                return_when=asyncio.FIRST_COMPLETED)
                    if watcher in done:
                        raise _Disconnected()
                    if task not in done:
                        raise OperationStopped('OPERATION_DEADLINE_EXCEEDED')
                    value = task.result()
                    budget.remaining()
                    code = 'OK'
                    await self._respond_result(operation,value,request_id,respond)
                except _ApiError as error:
                    code = error.code
                    await respond(error.status, {'version':self.response_version,'request_id':request_id,'error':code})
                except SharingHostError as error:
                    # Hide invalid identity, absent resources, forbidden scope,
                    # stale controls and token details behind one public denial.
                    unavailable = error.code in ('SHARING_HOST_DISABLED','SHARING_HOST_RETIRED',
                        'SHARING_HOST_NOT_STARTED','SHARING_HOST_PROTOCOL_FAILED','SHARING_HOST_OPERATION_FAILED')
                    code = 'SERVICE_UNAVAILABLE' if unavailable else 'ACCESS_DENIED'
                    await respond(503 if unavailable else 403,
                        {'version':self.response_version,'request_id':request_id,'error':code})
        except _Disconnected:
            code = 'CLIENT_DISCONNECTED'
            cancellation.cancel()
        except asyncio.CancelledError:
            code = 'REQUEST_CANCELLED'
            cancellation.cancel()
            raise
        except OperationStopped as error:
            code = error.code
            cancellation.cancel()
            # No late success/error body once the overall budget is exhausted.
        except OSError:
            code = 'RESPONSE_LOST'
            cancellation.cancel()
        except Exception:
            code = 'REQUEST_FAILED'
            if not response_started:
                try:
                    await respond(503, {'version':self.response_version,'request_id':request_id,
                                        'error':'SERVICE_UNAVAILABLE'})
                except (Exception, asyncio.CancelledError):
                    pass
            cancellation.cancel()
            # No raw provider text and no replacement body after response start.
        finally:
            if task is not None:
                if not task.done():
                    cancellation.cancel()
                await _join(task)
                self._tasks.discard(task)
            if watcher is not None:
                watcher.cancel()
                await _join(watcher)
            if admitted:
                self._active -= 1
                self._signals.discard(cancellation)
                self._requests.discard(asyncio.current_task())
            self._observations.append(ApiDeliveryObservation(request_id, operation,
                worker_requested, response_completed, status if response_started else None, code))
