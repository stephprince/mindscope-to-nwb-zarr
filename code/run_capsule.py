import argparse
from pathlib import Path
import shutil
import pandas as pd
from pynwb import NWBHDF5IO
from hdmf_zarr.nwb import NWBZarrIO
from nwbinspector import inspect_nwbfile_object, format_messages, save_report

# Define Code Ocean folder paths
data_folder = Path("../data/")
scratch_folder = Path("../scratch/")
results_folder = Path("../results/")

# Define dataset paths
VISBEH_OPHYS_BEHAVIOR_DATA_DIR = data_folder / "visual-behavior-ophys" / "behavior_sessions"
VISBEH_OPHYS_BEHAVIOR_OPHYS_DATA_DIR = data_folder / "visual-behavior-ophys" / "behavior_ophys_experiments"

VISBEH_OPHYS_METADATA_TABLES_DIR = data_folder / "cached_metadata" / "visual-behavior-ophys" / "project_metadata"
assert VISBEH_OPHYS_METADATA_TABLES_DIR.exists(), \
    f"Visual behavior ophys project metadata tables directory does not exist: {VISBEH_OPHYS_METADATA_TABLES_DIR}"


def convert_visual_behavior_2p_nwb_hdf5_to_zarr(hdf5_path: Path, zarr_path: Path):
    """Convert Visual Behavior 2P NWB file to Zarr format."""
    with NWBHDF5IO(str(hdf5_path), 'r') as read_io:
        # For initial testing of Code Ocean, just create an empty text file at the given zarr_path
        zarr_path.touch()
        # with NWBZarrIO(str(zarr_path), mode='w') as export_io:
        #     export_io.export(src_io=read_io, write_args=dict(link_data=False))
    print(f"Converted {hdf5_path} to Zarr format at {zarr_path}")


# Code Ocean workflow:
# Code Ocean will pass one NWB file from the Data Asset at a time (connection type: Default)
# Example usage:
# python code/run_capsule.py --input_nwb_dir ../data/visual-behavior-ophys/behavior_sessions --dataset "Visual Behavior 2P"

def run():
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_nwb_dir", type=Path, required=True)
    parser.add_argument("--dataset", type=str, required=True)
    args = parser.parse_args()
    dataset = args.dataset

    # Ensure results folder is empty
    for file in results_folder.iterdir():
        shutil.rmtree(file)
    print(f"Cleared results folder: {list(results_folder.iterdir())}")

    # Ensure there is only one NWB file in the input directory
    input_nwb_dir = args.input_nwb_dir
    print("INPUT NWB DIR", input_nwb_dir)
    assert input_nwb_dir.exists(), "Input NWB dir does not exist"
    nwb_files = [
        p
        for p in input_nwb_dir.iterdir()
        if p.name.endswith(".nwb")
    ]
    assert len(nwb_files) == 1, \
        f"Attach one base NWB file data at a time. {len(nwb_files)} found"
    input_nwb_path = nwb_files[0]
    print("INPUT NWB", input_nwb_path)

    # Output Zarr file path is just the input NWB file name with .zarr suffix in results folder
    result_zarr_path = results_folder / input_nwb_path.name.replace(".nwb", ".nwb.zarr")
    print("RESULT NWB", result_zarr_path)

    # Convert based on dataset type
    if dataset.lower() == "visual behavior 2p":
        convert_visual_behavior_2p_nwb_hdf5_to_zarr(
            hdf5_path=input_nwb_path,
            zarr_path=result_zarr_path
        )
    else:
        raise ValueError(f"Dataset not recognized: {dataset}")
    
    inspector_report_path = results_folder / f"{result_zarr_path.stem}_report.txt"
    print("INSPECTOR REPORT PATH", inspector_report_path)
    
    # Inspect output Zarr for validation errors
    # with NWBZarrIO(result_zarr_path, mode='r') as zarr_io:
    #     nwbfile = zarr_io.read()

    #     # Inspect nwb file with io object
    #     # NOTE - this does not run pynwb validation, will run that separately
    #     messages = list(inspect_nwbfile_object(nwbfile))

    #     # Format and print messages nicely
    #     if messages:
    #         formatted_messages = format_messages(
    #             messages=messages,
    #             levels=["importance", "file_path"],
    #             reverse=[True, False]
    #         )
    #         save_report(
    #             report_file_path=inspector_report_path, 
    #             formatted_messages=formatted_messages,
    #             overwrite=True,
    #         )
        
        # Validate file with IO object
        # TODO - waiting to fix hdmf-zarr related validation issues before including
        # validate(io=zarr_io)  


if __name__ == "__main__":
    run()