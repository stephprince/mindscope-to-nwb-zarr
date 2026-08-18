"""Script to generate AIND data schema JSON files for visual coding ephys dataset"""

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
