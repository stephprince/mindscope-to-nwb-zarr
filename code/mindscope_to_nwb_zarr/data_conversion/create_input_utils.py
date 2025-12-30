"""Utility functions for creating Code Ocean pipeline input files."""

from pathlib import Path
import shutil


def create_numbered_input_files(results_dir: Path, count: int) -> None:
    """Create numbered input files for Code Ocean pipeline.

    Creates empty files named 0, 1, 2, ..., N-1 in the specified directory.
    Clears the directory first if it already exists.

    Args:
        results_dir: Directory to create the input files in.
        count: Number of input files to create.
    """
    # Clear the results directory if it exists
    if results_dir.exists():
        print(f"Clearing existing directory {results_dir} ...")
        shutil.rmtree(results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    # Create files named 0, 1, 2, ..., N-1
    print(f"Creating {count} pipeline input files in {results_dir} ...")
    for i in range(count):
        input_path = results_dir / str(i)
        input_path.touch()

    print(f"Success: Created {count} pipeline input files")
