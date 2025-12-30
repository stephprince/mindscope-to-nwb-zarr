"""
Create empty input files for converting Visual Coding 2p ophys sessions in Code Ocean.

This script creates empty files named 0, 1, 2, ..., N-1 where N is
the number of rows in the visual_coding_2p_ophys_experiments.json file.
Each file corresponds to a row index in the ophys experiment metadata.

These files are used as inputs to the Code Ocean pipeline to determine which
sessions to convert. The actual NWB files are downloaded from DANDI during conversion.
The conversion script uses the input filename as the row index to look up
experiment information from the ophys experiment metadata JSON.
"""

from pathlib import Path

from mindscope_to_nwb_zarr.data_conversion.create_input_utils import create_numbered_input_files

NUMBER_OF_EXPERIMENTS = 1518


def main():
    # Output directory for pipeline input files
    results_dir = Path(__file__).parent.parent.parent.parent.parent / "data" / "visual-coding-ophys-inputs"
    create_numbered_input_files(results_dir, NUMBER_OF_EXPERIMENTS)


if __name__ == "__main__":
    main()
