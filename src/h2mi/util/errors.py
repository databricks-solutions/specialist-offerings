"""Error taxonomy and exit codes (design §9.2, §10)."""
from __future__ import annotations

EXIT_OK, EXIT_ERROR, EXIT_VALIDATION_MISMATCH, EXIT_LEASE_HELD, EXIT_BLOCKED, EXIT_CONFIG_INVALID = 0, 1, 2, 3, 4, 5


class H2miError(Exception):
    exit_code = EXIT_ERROR


class ConfigError(H2miError):
    """Invalid or missing configuration (exit 5)."""
    exit_code = EXIT_CONFIG_INVALID


class BlockedError(H2miError):
    """Scan finding, schema drift or integrity guard stops the table (exit 4)."""
    exit_code = EXIT_BLOCKED


class LeaseHeldError(H2miError):
    """Another run holds the table lease; the scheduler retries later (exit 3)."""
    exit_code = EXIT_LEASE_HELD


class ValidationMismatch(H2miError):
    """Source and target fingerprints differ (exit 2)."""
    exit_code = EXIT_VALIDATION_MISMATCH


class UnknownCommitOutcome(H2miError):
    """5xx / timeout during a commit. Never retried blindly: reconcile by snapshot tag (§8.7, OPS-04)."""


class SourceSnapshotExpired(H2miError):
    """The watermark snapshot no longer exists on the source (INC-11)."""
