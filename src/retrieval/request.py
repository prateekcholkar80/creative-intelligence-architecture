"""Validate the five agreed campaign retrieval inputs without cloud access.

Run: python -m src.retrieval.request --request config/retrieval_request.example.json
The request expresses user preferences. It does not establish product facts,
claim approval, manufacturer brand rules, or historical performance evidence.
The downstream reader must resolve canonical_sku against the existing BQ sku key.
"""
from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass, fields
from pathlib import Path


# This is the project's canonical identity range, not a generated product catalog.
CANONICAL_SKUS = frozenset(f'CI-SKU-{i:04d}' for i in range(1, 21))
OBJECTIVES = frozenset({'AWARENESS', 'CONSIDERATION', 'CONVERSION'})
CHANNELS = frozenset({'PAID_SOCIAL', 'PAID_SEARCH', 'EMAIL'})
VISUAL_REFERENCES = frozenset({'NONE', 'NAKED_AND_THRIVING'})


@dataclass(frozen=True, slots=True)
class RetrievalRequest:
    """One canonical SKU and four campaign preferences; no supplied evidence."""

    canonical_sku: str
    objective: str
    audience: str
    channel: str
    visual_reference: str

    def __post_init__(self) -> None:
        for field in fields(self):
            value = getattr(self, field.name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f'{field.name} must be a nonempty string')
        if self.canonical_sku not in CANONICAL_SKUS:
            raise ValueError('canonical_sku must be CI-SKU-0001 through CI-SKU-0020, exactly as stored')
        for name, choices in (('objective', OBJECTIVES), ('channel', CHANNELS),
                              ('visual_reference', VISUAL_REFERENCES)):
            if getattr(self, name) not in choices:
                raise ValueError(f'{name} must be one of: {", ".join(sorted(choices))}')
        object.__setattr__(self, 'audience', self.audience.strip())

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> RetrievalRequest:
        if not isinstance(payload, Mapping):
            raise ValueError('Retrieval request must be a JSON object')
        expected = {field.name for field in fields(cls)}
        missing, extra = expected-set(payload), set(payload)-expected
        if missing:
            raise ValueError(f'Missing request fields: {", ".join(sorted(missing))}')
        if extra:
            raise ValueError(f'Unsupported request fields: {", ".join(sorted(map(str, extra)))}')
        return cls(**payload)

    def to_dict(self) -> dict[str, str]:
        return asdict(self)

    @property
    def visual_reference_scope(self) -> str | None:
        return 'VISUAL_INSPIRATION_ONLY' if self.visual_reference != 'NONE' else None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--request', type=Path, required=True, help='JSON containing the five retrieval inputs')
    args = parser.parse_args()
    try:
        request = RetrievalRequest.from_dict(json.loads(args.request.read_text()))
    except (OSError, ValueError) as error:
        parser.error(str(error))
    print(json.dumps({'event': 'retrieval_request_validated', 'request': request.to_dict(),
        'visual_reference_scope': request.visual_reference_scope}, indent=2))


if __name__ == '__main__':
    main()
