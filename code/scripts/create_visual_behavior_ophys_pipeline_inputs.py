"""
Create empty input files for converting Visual Behavior 2p sessions in Code Ocean.

This script creates empty files named 0, 1, 2, ..., N-1 where N is
the number of rows in the behavior_session_table.csv. Each file
corresponds to a row index in the behavior session table. Each row in the behavior
session table contains information about any ophys experiments associated 
with that session.

These files are used as inputs to the Code Ocean pipeline to determine which
sessions to convert. The actual NWB files are downloaded from S3 during conversion.
The conversion script uses the input filename as the row index to look up
session information from the behavior session table.
"""

from pathlib import Path
import shutil

NUMBER_OF_SESSIONS = 4782


def main():
    # Output directory for pipeline input files
    results_dir = Path(__file__).parent.parent.parent / "data" / "visual-behavior-ophys-inputs"

    # Clear the results directory if it exists
    if results_dir.exists():
        print(f"Clearing existing directory {results_dir} ...")
        shutil.rmtree(results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    # Create files named 0, 1, 2, ..., N-1
    print(f"Creating {NUMBER_OF_SESSIONS} pipeline input files in {results_dir} ...")
    for i in range(NUMBER_OF_SESSIONS):
        input_path = results_dir / str(i)
        input_path.touch()

    print(f"Success: Created {NUMBER_OF_SESSIONS} pipeline input files")


if __name__ == "__main__":
    main()
