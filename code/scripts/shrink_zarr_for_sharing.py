"""Shrink an NWB-Zarr for sharing by stripping large array chunk data, keeping structure.

Each Zarr array is a directory holding tiny structural files (.zarray/.zattrs) plus its
data as chunk files (named like ``0.0``, ``12.3`` -- no leading dot). Deleting the chunk
files while keeping the ``.z*`` files leaves the array fully described (shape, dtype,
chunking, compressor, attributes) but with no stored data. PyNWB/hdmf-zarr open lazily,
so the file still opens and the whole structure (groups, tables, shapes, metadata) is
inspectable; only an explicit ``dataset[:]`` on a stripped array would touch the (now
missing) chunks. Missing chunks are not corruption -- Zarr treats them as the fill value.

Small arrays (metadata tables: electrodes, units columns, stimulus intervals, etc.) are
kept intact below the threshold so the metadata review is complete. Only the bulk raw
signals (LFP data, CSD, spike times, large timestamp arrays) are stripped.

Usage (from code/):
    uv run python scripts/shrink_zarr_for_sharing.py <zarr_path>            # dry-run
    uv run python scripts/shrink_zarr_for_sharing.py <zarr_path> --apply    # strip
    uv run python scripts/shrink_zarr_for_sharing.py <zarr_path> --apply --threshold-mb 5
"""
import argparse
from pathlib import Path


def array_dirs(zarr_root: Path):
    """Yield (array_dir, chunk_files, chunk_bytes) for every Zarr array under the root."""
    for zarray in zarr_root.rglob(".zarray"):
        d = zarray.parent
        chunk_files = [f for f in d.iterdir() if f.is_file() and not f.name.startswith(".")]
        chunk_bytes = sum(f.stat().st_size for f in chunk_files)
        yield d, chunk_files, chunk_bytes


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("zarr_path", type=Path)
    ap.add_argument("--threshold-mb", type=float, default=5.0,
                    help="strip arrays whose chunk data exceeds this (MB); default 5")
    ap.add_argument("--apply", action="store_true", help="actually delete (default: dry-run)")
    args = ap.parse_args()

    z = args.zarr_path
    if not (z / ".zgroup").exists():
        raise SystemExit(f"Not a Zarr root (no .zgroup): {z}")
    threshold = args.threshold_mb * 1_000_000

    arrays = sorted(array_dirs(z), key=lambda t: -t[2])
    total_before = sum(cb for _, _, cb in arrays)
    to_strip = [(d, files, cb) for d, files, cb in arrays if cb > threshold]
    keep = [(d, files, cb) for d, files, cb in arrays if cb <= threshold]

    print(f"Zarr: {z}")
    print(f"Total chunk data: {total_before/1e9:.2f} GB across {len(arrays)} arrays")
    print(f"Threshold: {args.threshold_mb} MB  ->  strip {len(to_strip)} arrays, keep {len(keep)}\n")
    print(f"{'GB':>8} {'chunks':>7}  array (to STRIP)")
    stripped_bytes = 0
    for d, files, cb in to_strip:
        stripped_bytes += cb
        print(f"{cb/1e9:8.3f} {len(files):7d}  {d.relative_to(z)}")

    kept_bytes = sum(cb for _, _, cb in keep)
    print(f"\nWould remove {stripped_bytes/1e9:.2f} GB; keep {kept_bytes/1e6:.1f} MB of small "
          f"arrays + all .z* structural files.")
    print(f"Estimated size after: ~{kept_bytes/1e6:.0f} MB (+ tiny .z* metadata).")

    if not args.apply:
        print("\nDry-run. Re-run with --apply to delete the chunk files above.")
        return

    n_deleted = 0
    for d, files, cb in to_strip:
        for f in files:
            f.unlink()
            n_deleted += 1
    print(f"\nAPPLIED: deleted {n_deleted} chunk files ({stripped_bytes/1e9:.2f} GB).")
    remaining = sum(f.stat().st_size for f in z.rglob("*") if f.is_file())
    print(f"Zarr size now: {remaining/1e6:.1f} MB")


if __name__ == "__main__":
    main()
