"""On-disk cache for AIND metadata-service responses (subject / procedures).

The metadata-service response for a given ``subject_id`` does not change between runs,
and the same subject appears in many sessions (the Visual Behavior Ophys dataset has
~4800 sessions but only ~107 subjects), so re-fetching per session dominates batch
runtime. Caching the raw response dict by ``subject_id`` turns thousands of slow
network calls into ~one per subject.

What is cached is the ``raw_data`` dict passed to ``Subject(**raw_data)`` /
``Procedures(**raw_data)`` -- i.e. the metadata-service response after the
raw-parse/genotype fixups, serialized JSON-safe (``model_dump(mode="json")``). A cache
hit therefore reconstructs an identical model. Per-session cross-checks against the NWB
file still run on every session; only the network fetch is skipped.

The cache location defaults to ``code/scratch/aind_metadata_cache/`` (git-ignored,
writable everywhere) and can be overridden with the ``AIND_METADATA_CACHE_DIR``
environment variable so a run can point at a shared/pre-populated cache.
"""
import json
import os
from pathlib import Path

# parents[2] == the code/ directory (this file is code/mindscope_to_nwb_zarr/aind_data_schema/).
_DEFAULT_CACHE_DIR = Path(__file__).resolve().parents[2] / "scratch" / "aind_metadata_cache"

# Cache kinds (subdirectories).
SUBJECT = "subject"
PROCEDURES = "procedures"


def cache_dir() -> Path:
    """The cache root (overridable via AIND_METADATA_CACHE_DIR)."""
    return Path(os.environ.get("AIND_METADATA_CACHE_DIR", _DEFAULT_CACHE_DIR))


def _cache_path(kind: str, subject_id) -> Path:
    return cache_dir() / kind / f"{subject_id}.json"


def load_cached(kind: str, subject_id) -> dict | None:
    """Return the cached raw_data dict for (kind, subject_id), or None if not cached.

    A corrupt/unreadable cache file is treated as a miss (returns None) so the caller
    re-fetches rather than crashing.
    """
    path = _cache_path(kind, subject_id)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def store_cached(kind: str, subject_id, raw_data: dict) -> None:
    """Cache raw_data for (kind, subject_id) via an atomic write.

    The temp file is per-process so concurrent workers writing the same subject cannot
    corrupt each other's file; ``os.replace`` swaps it into place atomically.
    """
    path = _cache_path(kind, subject_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.stem}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(raw_data), encoding="utf-8")
    os.replace(tmp, path)
