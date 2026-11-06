"""Immutable algorithm/configuration versions (``policy_versions`` table).

A policy's ID is derived from its content, so a changed config always yields a
new ID; datasets pin the IDs they used.
"""

from __future__ import annotations

import hashlib
from typing import Any

from pydantic import Field, model_validator

from marketagg.domain.primitives import CanonicalModel, Sha256Hex, canonical_dumps


class PolicyVersion(CanonicalModel):
    id: str = Field(min_length=1, max_length=128)
    kind: str = Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")
    config: dict[str, Any]
    code_sha: str = Field(min_length=1)
    content_sha256: Sha256Hex

    @classmethod
    def create(cls, kind: str, config: dict[str, Any], code_sha: str) -> PolicyVersion:
        digest = policy_content_sha256(kind, config)
        return cls(
            id=f"{kind}:{digest[:16]}",
            kind=kind,
            config=config,
            code_sha=code_sha,
            content_sha256=digest,
        )

    @model_validator(mode="after")
    def _hash_matches(self) -> PolicyVersion:
        if policy_content_sha256(self.kind, self.config) != self.content_sha256:
            raise ValueError("content_sha256 does not match kind+config")
        return self


def policy_content_sha256(kind: str, config: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_dumps({"kind": kind, "config": config}).encode()).hexdigest()
