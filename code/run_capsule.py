import argparse
from pathlib import Path
import shutil

from mindscope_to_nwb_zarr.data_conversion.conversion_utils import inspect_zarr_file

print("STARTING CODE OCEAN CAPSULE RUN")

# Define Code Ocean folder paths
code_folder = Path(__file__).parent
data_folder = Path("../data/")
scratch_dir = Path("../scratch/")


def generate_metadata_for_dataset(dataset: str, data_folder: Path, results_dir: Path) -> None:
    """
    Generate session metadata for all files in the data directory.

    Parameters
    ----------
    dataset : str
        Dataset type name
    data_folder : Path
        Path to data directory containing NWB files
    results_dir : Path
        Path to directory to save output metadata JSON files
    """
    if dataset.lower() == "visual behavior ephys":
        from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ephys.metadata_generation import generate_all_session_metadata
        generate_all_session_metadata(data_folder, results_dir)
    elif dataset.lower() == "visual behavior ophys":
        from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ophys.metadata_generation import generate_all_session_metadata
        generate_all_session_metadata(data_folder, results_dir)
    elif dataset.lower() == "visual coding ephys":
        from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.metadata_generation import generate_all_session_metadata
        generate_all_session_metadata(data_folder, results_dir)
    elif dataset.lower() == "visual coding ophys":
        from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ophys.metadata_generation import generate_all_session_metadata
        generate_all_session_metadata(data_folder, results_dir)
    else:
        raise ValueError(f"Unsupported dataset type: {dataset}")


def run():
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=str, required=True)
    parser.add_argument("--results_dir", type=str, default="../results/")
    parser.add_argument("--metadata", type=str, choices=["True", "False"], default="False", help=(
        "Generate session metadata for all files in data directory and do NOT generate Zarr files"
    ))

    args = parser.parse_args()
    dataset = args.dataset
    results_dir = Path(args.results_dir)

    # Handle metadata generation mode
    if args.metadata == "True":
        generate_metadata_for_dataset(dataset, data_folder, results_dir)
        return

    # Ensure results folder is empty
    for item in results_dir.iterdir():
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()
    print(f"Cleared results folder: {list(results_dir.iterdir())}")

    # Convert NWB files based on dataset type
    if dataset.lower() == "visual behavior ephys":
        from mindscope_to_nwb_zarr.data_conversion.visual_behavior_ephys.run_conversion import convert_visual_behavior_ephys_hdf5_to_zarr
        result_zarr_path = convert_visual_behavior_ephys_hdf5_to_zarr(results_dir=results_dir, scratch_dir=scratch_dir)
    elif dataset.lower() == "visual behavior ophys":
        from mindscope_to_nwb_zarr.data_conversion.visual_behavior_ophys.run_conversion import convert_visual_behavior_ophys_hdf5_to_zarr
        result_zarr_path = convert_visual_behavior_ophys_hdf5_to_zarr(results_dir=results_dir, scratch_dir=scratch_dir)
    elif dataset.lower() == "visual coding ephys":
        from mindscope_to_nwb_zarr.data_conversion.visual_coding_ephys.run_conversion import convert_visual_coding_ephys_hdf5_to_zarr
        result_zarr_path = convert_visual_coding_ephys_hdf5_to_zarr(results_dir=results_dir, scratch_dir=scratch_dir)
    elif dataset.lower() == "visual coding ophys":
        from mindscope_to_nwb_zarr.data_conversion.visual_coding_ophys.run_conversion import convert_visual_coding_ophys_hdf5_to_zarr
        result_zarr_path = convert_visual_coding_ophys_hdf5_to_zarr(results_dir=results_dir, scratch_dir=scratch_dir)
    else:
        raise ValueError(f"Unsupported dataset type: {dataset}")

    # Validate and inspect resulting Zarr file
    inspector_report_path = result_zarr_path.with_suffix('.inspector_report.txt')
    print(f"Inspecting resulting Zarr file {result_zarr_path} ...")
    inspect_zarr_file(zarr_path=result_zarr_path, inspector_report_path=inspector_report_path)


if __name__ == "__main__":
    run()
