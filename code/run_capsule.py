import argparse
from pathlib import Path
import shutil

from mindscope_to_nwb_zarr.data_conversion.conversion_utils import inspect_zarr_file

print("STARTING CODE OCEAN CAPSULE RUN")

# Define Code Ocean folder paths
code_folder = Path(__file__).parent
data_folder = Path("../data/")
scratch_dir = Path("../scratch/")


# NOTE: AIND metadata (data_description / subject / acquisition / procedures / instrument)
# is generated LOCALLY, not in this capsule. The AIND metadata service is only reachable on
# the Allen VPN, which Code Ocean does not have, so the metadata is produced by the
# scripts/run_all_*.py generators (which stream the NWBs and hit the metadata service) and
# uploaded to Code Ocean as data assets that feed the Zarr conversion below. This capsule
# only runs the HDF5 -> Zarr conversion.


def run():
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=str, required=True)
    parser.add_argument("--results_dir", type=str, default="../results/")

    args = parser.parse_args()
    dataset = args.dataset
    results_dir = Path(args.results_dir)

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

    # A conversion may be a no-op (e.g. a Visual Coding Ephys test job whose mounted zip is
    # not the hardcoded target); there is nothing to inspect in that case.
    if result_zarr_path is None:
        print("No Zarr produced for this job (no-op); nothing to inspect.")
        return

    # Validate and inspect resulting Zarr file. Write the report into a qc/ subfolder
    # adjacent to the Zarr (and, for Visual Coding Ephys, the unzipped metadata JSONs).
    # NOTE: Commented out for live runs
    # qc_dir = result_zarr_path.parent / "qc"
    # qc_dir.mkdir(parents=True, exist_ok=True)
    # inspector_report_path = qc_dir / (result_zarr_path.stem + ".inspector_report.txt")
    # print(f"Inspecting resulting Zarr file {result_zarr_path} ...")
    # inspect_zarr_file(zarr_path=result_zarr_path, inspector_report_path=inspector_report_path)


if __name__ == "__main__":
    run()
