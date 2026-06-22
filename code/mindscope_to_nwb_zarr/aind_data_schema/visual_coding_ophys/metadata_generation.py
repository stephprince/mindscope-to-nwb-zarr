"""Script to generate AIND data schema JSON files for visual coding ophys dataset.

NWB files are streamed from the DANDI Archive using fsspec (no download required)
instead of from the Allen Institute S3 bucket for this dataset (s3://allen-brain-observatory,
path: visual-coding-2p) because the S3 bucket contians NWB 1.0.5 files and the DANDI versions
of the NWB files were recently updated to NWB 2.7. As far as we know, the Allen Institute
does not maintain a separate S3 bucket containing the updated NWB files, so streaming
from DANDI is the only way to access the updated NWB files for this dataset.

Source data on DANDI Archive:
    dandiset 000728, version 0.240827.1809

Session structure (on DANDI):
    sub-{specimen_id}/
        sub-{specimen_id}_ses-{experiment_id}-{StimX}_behavior+image+ophys.nwb  - Processed data
        sub-{specimen_id}_ses-{experiment_id}-{StimX}_ophys.nwb                 - Raw 2p data

Only the processed NWB file is needed for metadata generation.
"""

import traceback
import pandas as pd

from pathlib import Path
import h5py
from pynwb import NWBHDF5IO

from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ophys.acquisition import generate_acquisition
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ophys.data_description import generate_data_description
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ophys.subject import fetch_subject_from_aind_metadata_service
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ophys.procedures import fetch_procedures_from_aind_metadata_service
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ophys.instrument import generate_instrument

DANDISET_ID = "000728"
DANDISET_VERSION = "0.240827.1809"

# Mapping from stimulus_name in metadata JSON to stim suffix for DANDI asset path
STIMULUS_NAME_TO_SUFFIX = {
    "three_session_A": "StimA",
    "three_session_B": "StimB",
    "three_session_C": "StimC",
    "three_session_C2": "StimC2",
}


def get_dandi_base_name(experiment_metadata: pd.Series) -> str:
    """Build the DANDI asset base name (filename stem without the modality suffix).

    The base name follows the pattern:
        sub-{specimen_id}_ses-{id}-{StimX}

    Where StimX is "StimA", "StimB", "StimC", or "StimC2" based on stimulus_name,
    e.g. "sub-639389525_ses-653123586-StimC2".

    Args:
        experiment_metadata: A row from the ophys experiment metadata DataFrame.

    Returns:
        The DANDI asset base name.
    """
    specimen_id = experiment_metadata['specimen_id']
    experiment_id = experiment_metadata['id']
    stimulus_name = experiment_metadata['stimulus_name']

    stim_suffix = STIMULUS_NAME_TO_SUFFIX.get(stimulus_name)
    if stim_suffix is None:
        raise ValueError(f"Unknown stimulus_name: {stimulus_name}")

    return f"sub-{specimen_id}_ses-{experiment_id}-{stim_suffix}"


def get_dandi_asset_path(experiment_metadata: pd.Series) -> str:
    """Build DANDI asset path for the processed NWB file from experiment metadata.

    Asset paths follow the pattern:
        sub-{specimen_id}/sub-{specimen_id}_ses-{id}-{StimX}_behavior+image+ophys.nwb

    Args:
        experiment_metadata: A row from the ophys experiment metadata DataFrame.

    Returns:
        The DANDI asset path for the processed NWB file.
    """
    specimen_id = experiment_metadata['specimen_id']
    base_name = get_dandi_base_name(experiment_metadata)

    return f"sub-{specimen_id}/{base_name}_behavior+image+ophys.nwb"


def stream_nwb_from_dandi(asset_path: str):
    """Stream an NWB file from DANDI using fsspec (no download required).

    Args:
        asset_path: DANDI asset path for the NWB file.

    Returns:
        Tuple of (nwbfile, io, h5_file, file_handle) - the NWB file object, IO handle,
        h5py File object, and remfile file handle.
        Caller is responsible for closing all handles.
    """
    import remfile
    from dandi.dandiapi import DandiAPIClient

    with DandiAPIClient() as client:
        dandiset = client.get_dandiset(DANDISET_ID, DANDISET_VERSION)
        asset = dandiset.get_asset_by_path(asset_path)
        if not asset:
            raise RuntimeError(
                f"No asset found for {asset_path} "
                f"in DANDI dandiset {DANDISET_ID} version {DANDISET_VERSION}"
            )
        s3_url = asset.get_content_url(follow_redirects=1, strip_query=True)

    # Stream with remfile, which does block-level caching/prefetch over HTTP. Reading
    # the HDF5 metadata tree issues many small byte-range reads; remfile coalesces them
    # into a few larger requests, which is far faster and more robust to connection
    # resets than raw fsspec http (which makes one round-trip per HDF5 object).
    file_handle = remfile.File(s3_url)

    # Wrap with h5py.File, then pass to NWBHDF5IO
    h5_file = h5py.File(file_handle, "r")
    io = NWBHDF5IO(file=h5_file)
    nwbfile = io.read()

    return nwbfile, io, h5_file, file_handle


def generate_session_metadata(nwbfile, session_info: pd.Series, output_dir: Path) -> None:
    """
    Process a single NWB file and generate AIND data schema JSON files.

    Parameters
    ----------
    nwbfile : NWBFile
        The NWB file object (already opened)
    session_info : pd.Series
        Session metadata row from the ophys experiment metadata
    output_dir : Path
        Path to directory to save output JSON files
    """
    # Generate metadata models. The DataDescription name (also used as the per-session
    # output folder name below) is set to the DANDI asset base name.
    base_name = get_dandi_base_name(session_info)
    data_description = generate_data_description(nwbfile, session_info, name=base_name)
    subject = None  # fetch_subject_from_aind_metadata_service(nwbfile, session_info)
    acquisition = generate_acquisition(nwbfile, session_info)
    procedures = None  # fetch_procedures_from_aind_metadata_service(nwbfile, session_info)
    # instrument is None when the session's rig cannot be resolved (older
    # experiments with no ophys_session_id in storage_directory) or has no
    # instrument definition; such sessions are skipped below.
    instrument = generate_instrument(nwbfile, session_info)
    metadata_models = [data_description, subject, acquisition, procedures, instrument]

    # Save the metadata files
    session_output_dir = output_dir / data_description.name
    session_output_dir.mkdir(parents=True, exist_ok=True)
    for model in metadata_models:
        if model is not None:
            serialized = model.model_dump_json()
            deserialized = model.model_validate_json(serialized)
            deserialized.write_standard_file(output_directory=session_output_dir)


def generate_all_session_metadata(data_dir: Path, results_dir: Path) -> None:
    """
    Iterate through all sessions and generate session metadata by streaming from DANDI.

    The S3 bucket s3://allen-brain-observatory is mounted at data_dir/allen-brain-observatory.
    Reads experiment metadata from allen-brain-observatory/visual-coding-2p/ophys_experiments.json.

    Parameters
    ----------
    data_dir : Path
        Path to data directory where S3 bucket is mounted
    results_dir : Path
        Path to directory to save output metadata JSON files
    """
    output_dir = results_dir / "visual-coding-ophys-metadata"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load ophys experiment metadata from mounted S3 bucket
    metadata_path = data_dir / "allen-brain-observatory" / "visual-coding-2p" / "ophys_experiments.json"
    ophys_experiment_metadata = pd.read_json(metadata_path)

    print(f"Found {len(ophys_experiment_metadata)} ophys experiments")

    for row_index, experiment_row in ophys_experiment_metadata.iterrows():
        experiment_id = experiment_row['id']
        print(f"\nProcessing experiment {experiment_id} (row {row_index}) ...")

        try:
            # Build DANDI asset path
            asset_path = get_dandi_asset_path(experiment_row)
            print(f"  Streaming from DANDI: {asset_path}")

            # Stream NWB file from DANDI
            nwbfile, io, h5_file, file_handle = stream_nwb_from_dandi(asset_path)

            try:
                # Generate metadata
                generate_session_metadata(
                    nwbfile=nwbfile,
                    session_info=experiment_row,
                    output_dir=output_dir,
                )
            finally:
                # Clean up handles
                io.close()
                h5_file.close()
                file_handle.close()

        except Exception as e:
            print(f"Error generating metadata for experiment {experiment_id}: {e}")
            traceback.print_exc()
            continue

    print("\nDone generating metadata!")
