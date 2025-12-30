"""
Create empty input files for converting Visual Coding Neuropixels (ephys) sessions in Code Ocean.

This script creates empty files named 0, 1, 2, ..., N-1 where N is
the number of rows in the sessions.csv file from S3. Each file
corresponds to a row index in the sessions table.

These files are used as inputs to the Code Ocean pipeline to determine which
sessions to convert. The actual NWB files are downloaded from S3 during conversion.
The conversion script uses the input filename as the row index to look up
session information from the sessions.csv file.
"""

from pathlib import Path

from mindscope_to_nwb_zarr.data_conversion.create_input_utils import create_numbered_input_files

NUMBER_OF_SESSIONS = 58


def main():
    # Output directory for pipeline input files
    results_dir = Path(__file__).parent.parent.parent.parent.parent / "data" / "visual-coding-ephys-inputs"
    create_numbered_input_files(results_dir, NUMBER_OF_SESSIONS)


if __name__ == "__main__":
    main()
