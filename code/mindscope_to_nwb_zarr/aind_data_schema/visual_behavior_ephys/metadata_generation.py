"""Script to generate AIND data schema JSON files for visual behavior neuropixels dataset"""

import pandas as pd

from pathlib import Path
from pynwb import NWBHDF5IO

from mindscope_to_nwb_zarr.aind_data_schema.utils import zip_session_metadata  # noqa: F401  (re-exported)
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ephys.acquisition import generate_acquisition
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ephys.data_description import generate_data_description
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ephys.subject import fetch_subject_from_aind_metadata_service
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ephys.procedures import fetch_procedures_from_aind_metadata_service
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ephys.instrument import generate_instrument


def generate_session_metadata(nwb_file_path: Path, session_info: pd.Series, output_dir: Path,
                              include_procedures: bool = True):
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
    include_procedures : bool, optional
        Whether to fetch and write procedures.json. Defaults to True. Set to False to skip
        procedures (e.g. while the metadata-service procedures endpoint is unavailable);
        the other models are still generated and procedures can be added on a later run.
    """
    # Open the NWB explicitly (rather than pynwb.read_nwb) so the file handle is closed
    # after generation; the batch loop opens thousands of files and would otherwise leak.
    io = NWBHDF5IO(str(nwb_file_path), mode="r", load_namespaces=True)
    try:
        nwbfile = io.read()

        # Validate that session description matches metadata
        assert nwbfile.session_description == session_info['session_type'], \
            f"Session description mismatch: {nwbfile.session_description} != {session_info['session_type']}"

        # Generate metadata models. subject and procedures are fetched from the AIND metadata
        # service (cached by 6-digit mouse id); both fail loudly on an unexpected outcome
        # (unreachable service, 404, or an NWB/LIMS disagreement) rather than returning None.
        data_description = generate_data_description(nwbfile, session_info)
        subject = fetch_subject_from_aind_metadata_service(nwbfile, session_info)
        acquisition = generate_acquisition(nwbfile, session_info)
        procedures = fetch_procedures_from_aind_metadata_service(nwbfile, session_info) if include_procedures else None
        instrument = generate_instrument(session_info)  # NP rig (+ lick spout) or BEH box, by equipment_name
        metadata_models = [data_description, subject, acquisition, procedures, instrument]

        # Save the metadata files
        Path(output_dir / data_description.name).mkdir(parents=True, exist_ok=True)
        for model in metadata_models:
            if model is not None:
                serialized = model.model_dump_json()
                deserialized = model.model_validate_json(serialized)
                deserialized.write_standard_file(output_directory=output_dir / data_description.name)
    finally:
        io.close()
