"""Persistence errors with operational meaning."""

from __future__ import annotations


class PersistenceError(RuntimeError):
    pass


class StaleLease(PersistenceError):
    """The job lease/fencing token no longer belongs to this worker; nothing committed."""


class PartitionMissing(PersistenceError):
    """A required monthly partition is not provisioned (readiness failure, not auto-fixed)."""


class IdentityConflict(PersistenceError):
    """Source identity is ambiguous; the affected rows are quarantined, never guessed."""


class ClockSkewError(PersistenceError):
    """Application and database clocks differ by more than the allowed skew."""


class InvariantViolation(PersistenceError):
    """A cross-row invariant (correction parent, source, basis, ...) was violated."""
