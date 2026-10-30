"""Batch packing and the credential-aware batch cap (design §8.4, INI-02, R11)."""
from __future__ import annotations


def pack(parts: list[tuple[str, int]], target_bytes: int) -> list[list[str]]:
    """First-fit-decreasing packing of (partition, bytes) into batches of about `target_bytes`.
    A single partition larger than the target gets its own batch (one commit per batch, INC-09)."""
    batches: list[tuple[int, list[str]]] = []
    for name, size in sorted(parts, key=lambda p: -p[1]):
        for b in batches:
            if b[0] + size <= target_bytes:
                b[1].append(name)
                batches[batches.index(b)] = (b[0] + size, b[1])
                break
        else:
            batches.append((size, [name]))
    return [names for _, names in batches]


def cap_batch_bytes(target_bytes: int, throughput_bytes_per_s: float | None, minutes: int = 40) -> int:
    """Keep every batch under the ~60 min vended-credential lifetime (R11): min(target, throughput x 40 min)."""
    if not throughput_bytes_per_s:
        return target_bytes
    return min(target_bytes, int(throughput_bytes_per_s * minutes * 60))
