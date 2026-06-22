"""List Visual Coding 2P ophys experiment IDs that have no findable ophys_session_id.

The conversion pipeline resolves a session's rig (and thus its instrument) by
recovering the ophys_session_id from the experiment's ``storage_directory``,
which embeds an ``ophys_session_<id>`` path component for newer LIMS prod
versions. Older experiments store only ``ophys_experiment_<id>`` and have no
recoverable session id anywhere in the local/public metadata, so they cannot be
assigned a rig.

This script scans ``ophys_experiments.json`` and writes the list of experiment
``id`` values whose session id is *not* findable (i.e.
``extract_ophys_session_id`` returns ``None``) to a CSV file, one id per row
(with an ``experiment_id`` header).

Usage
-----
    uv run python scripts/list_experiments_without_session_id.py \
        [--experiments PATH] [--output PATH]
"""

import argparse
import csv
import json
from pathlib import Path

from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ophys.instrument import (
    extract_ophys_session_id,
)

# Default location of the experiment metadata (mounted S3 bucket layout).
_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_EXPERIMENTS = (
    _REPO_ROOT / "data" / "allen-brain-observatory" / "visual-coding-2p" / "ophys_experiments.json"
)
_DEFAULT_OUTPUT = (
    Path(__file__).resolve().parents[1] / "reference" / "experiments_without_session_id.csv"
)


def find_experiments_without_session_id(experiments_path: Path) -> tuple[list[int], int]:
    """Return (sorted experiment ids with no findable session id, total count)."""
    with open(experiments_path) as f:
        experiments = json.load(f)

    missing = [
        experiment["id"]
        for experiment in experiments
        if extract_ophys_session_id(experiment.get("storage_directory")) is None
    ]
    return sorted(missing), len(experiments)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--experiments",
        type=Path,
        default=_DEFAULT_EXPERIMENTS,
        help="Path to ophys_experiments.json (default: mounted S3 bucket copy).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=_DEFAULT_OUTPUT,
        help="Path to write the JSON list of experiment ids (default: %(default)s).",
    )
    args = parser.parse_args()

    missing, total = find_experiments_without_session_id(args.experiments)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["experiment_id"])
        writer.writerows([experiment_id] for experiment_id in missing)

    print(f"Read {total} experiments from {args.experiments}")
    print(f"{len(missing)} experiments have no findable session id "
          f"({total - len(missing)} resolvable)")
    print(f"Wrote {len(missing)} experiment ids to {args.output}")


if __name__ == "__main__":
    main()
