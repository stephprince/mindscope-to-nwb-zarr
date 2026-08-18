"""Script to generate AIND data schema JSON files for visual coding ephys dataset"""

import traceback
import pandas as pd

from pathlib import Path
from pynwb import read_nwb

from mindscope_to_nwb_zarr.aind_data_schema.utils import zip_session_metadata  # noqa: F401  (re-exported)
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.acquisition import generate_acquisition
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.data_description import generate_data_description
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.subject import (
    fetch_subject_from_aind_metadata_service,
    cross_check_mouse_id,
)
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.procedures import fetch_procedures_from_aind_metadata_service
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.instrument import generate_instrument

# Path to session metadata CSV files from the data folder
SESSIONS_CSV_PATH = "allen-brain-observatory/visual-coding-neuropixels/ecephys-cache/sessions.csv"

# Path to subject mapping JSON file relative to code directory
CODE_DIR = Path(__file__).parent.parent.parent.parent
SUBJECT_MAPPING_PATH = CODE_DIR / "reference" / "visual_coding_ephys_subject_mapping.json"


def generate_session_metadata(nwb_file_path: Path, session_info: pd.Series, output_dir: Path,
                              zip_output: bool = False) -> None:
    """
    Process a single NWB file and generate AIND data schema JSON files.

    Parameters
    ----------
    nwb_file_path : Path
        Path to the NWB file
    session_info : pd.Series
        Session metadata row from the session table
    output_dir : Path
        Path to directory to save output JSON files
    zip_output : bool, optional
        When True, the session's metadata files are bundled into a single
        ``<data asset name>.zip`` in ``output_dir`` and the loose per-session folder is
        removed (see ``zip_session_metadata``), so ``output_dir`` holds only one zip per
        session. Defaults to False (the loose per-session folder is kept).
    """
    # Read NWB file
    nwbfile = read_nwb(nwb_file_path)

    # Validate that session type matches metadata
    assert nwbfile.stimulus_notes == session_info['session_type'], \
        f"Session type mismatch: {nwbfile.stimulus_notes} != {session_info['session_type']}"

    # Cross-check the mouse id across the experiment CSV and the subject mapping
    cross_check_mouse_id(nwbfile, session_info, subject_mapping_path=SUBJECT_MAPPING_PATH)

    # Generate metadata models
    data_description = generate_data_description(nwbfile, session_info)
    subject = fetch_subject_from_aind_metadata_service(nwbfile, session_info, subject_mapping_path=SUBJECT_MAPPING_PATH)
    acquisition = generate_acquisition(nwbfile, session_info)
    procedures = fetch_procedures_from_aind_metadata_service(nwbfile, subject_mapping_path=SUBJECT_MAPPING_PATH)
    instrument = generate_instrument(session_info)
    metadata_models = [data_description, subject, acquisition, procedures, instrument]

    # Save the metadata files
    session_dir = output_dir / data_description.name
    session_dir.mkdir(parents=True, exist_ok=True)
    for model in metadata_models:
        if model is not None:
            serialized = model.model_dump_json()
            deserialized = model.model_validate_json(serialized)
            deserialized.write_standard_file(output_directory=session_dir)

    # Optionally collapse the per-session folder into a single zip in output_dir.
    if zip_output:
        zip_session_metadata(session_dir, output_dir)


def generate_all_session_metadata(data_dir: Path, results_dir: Path, zip_output: bool = False) -> None:
    """
    Iterate through all sessions in the mounted data directory and generate session metadata.

    The S3 bucket s3://allen-brain-observatory is mounted at data_dir/allen-brain-observatory.
    Iterates through all sessions in the sessions.csv and generates metadata JSON files.

    Parameters
    ----------
    data_dir : Path
        Path to data directory where S3 bucket is mounted
    results_dir : Path
        Path to directory to save output metadata JSON files
    zip_output : bool, optional
        When True, each session's metadata files are bundled into a single per-session zip
        so the output directory holds only one zip per session (see
        ``generate_session_metadata``). Defaults to False.
    """
    output_dir = results_dir / "visual-coding-neuropixels-metadata"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Mounted data path
    mounted_data_path = data_dir / "allen-brain-observatory" / "visual-coding-neuropixels" / "ecephys-cache"

    # Load sessions table
    sessions_df = pd.read_csv(data_dir / SESSIONS_CSV_PATH)

    print(f"Found {len(sessions_df)} sessions")

    for row_index, session_row in sessions_df.iterrows():
        session_id = int(session_row['id'])
        print(f"\nProcessing session {session_id} (row {row_index}) ...")

        # Build NWB file path
        session_dir = mounted_data_path / f"session_{session_id}"
        nwb_filename = f"session_{session_id}.nwb"
        nwb_file_path = session_dir / nwb_filename

        if not nwb_file_path.exists():
            print(f"NWB file not found: {nwb_file_path}. Skipping.")
            continue

        # Generate metadata
        try:
            generate_session_metadata(
                nwb_file_path=nwb_file_path,
                session_info=session_row,
                output_dir=output_dir,
                zip_output=zip_output,
            )
        except Exception as e:
            print(f"Error generating metadata for session {session_id}: {e}")
            traceback.print_exc()
            continue

    print("\nDone generating metadata!")
