"""Bounded JSON frames over owned nonblocking local sockets; no pickle or logs."""
from __future__ import annotations

import json
import select
import struct

MAX_FRAME_BYTES = 65536


class ChannelError(RuntimeError):
    def __init__(self):
        super().__init__('SHARING_HOST_PROTOCOL_FAILED')


def _unique(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ChannelError()
        value[key] = item
    return value


def encode(value: dict) -> bytes:
    try:
        if type(value) is not dict:
            raise ChannelError()
        body = json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(',', ':')).encode()
        if not 1 <= len(body) <= MAX_FRAME_BYTES:
            raise ChannelError()
        return struct.pack('!I', len(body)) + body
    except (ValueError, TypeError, RecursionError):
        raise ChannelError() from None


def send(channel, value: dict, remaining) -> None:
    data = memoryview(encode(value))
    while data:
        wait = min(.02, remaining())
        if not select.select([], [channel], [], wait)[1]:
            continue
        try:
            count = channel.send(data)
        except BlockingIOError:
            continue
        if count <= 0:
            raise ChannelError()
        data = data[count:]
    remaining()


def receive(channel, remaining, *, poll_seconds=.02) -> dict:
    def read(size):
        data = bytearray()
        while len(data) < size:
            wait = min(poll_seconds, remaining())
            if not select.select([channel], [], [], wait)[0]:
                continue
            try:
                part = channel.recv(size - len(data))
            except BlockingIOError:
                continue
            if not part:
                raise ChannelError()
            data.extend(part)
        remaining()
        return bytes(data)
    size = struct.unpack('!I', read(4))[0]
    if not 1 <= size <= MAX_FRAME_BYTES:
        raise ChannelError()
    try:
        value = json.loads(read(size), object_pairs_hook=_unique,
                           parse_constant=lambda _: (_ for _ in ()).throw(ChannelError()))
    except (ValueError, TypeError, RecursionError):
        raise ChannelError() from None
    if type(value) is not dict:
        raise ChannelError()
    return value
