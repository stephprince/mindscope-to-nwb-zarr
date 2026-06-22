"""List all Visual Coding 2P ophys experiment IDs from ophys_experiments.json.

Writes the sorted list of every experiment ``id`` value to a CSV file, one id
per row (with an ``experiment_id`` header).

Usage
-----
    uv run python scripts/list_all_experiment_ids.py [--experiments PATH] [--output PATH]
"""

import argparse
import csv
import json
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_EXPERIMENTS = (
    _REPO_ROOT / "data" / "allen-brain-observatory" / "visual-coding-2p" / "ophys_experiments.json"
)
_DEFAULT_OUTPUT = Path(__file__).resolve().parents[1] / "reference" / "all_experiment_ids.csv"


def list_all_experiment_ids(experiments_path: Path) -> list[int]:
    """Return the sorted list of every experiment id in the metadata file."""
    with open(experiments_path) as f:
        experiments = json.load(f)
    return sorted(experiment["id"] for experiment in experiments)


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

    ids = list_all_experiment_ids(args.experiments)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["experiment_id"])
        writer.writerows([experiment_id] for experiment_id in ids)

    print(f"Read {len(ids)} experiments from {args.experiments}")
    print(f"Wrote {len(ids)} experiment ids to {args.output}")


if __name__ == "__main__":
    main()
