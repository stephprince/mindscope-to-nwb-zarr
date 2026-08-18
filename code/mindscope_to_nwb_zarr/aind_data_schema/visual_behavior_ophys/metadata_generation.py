"""Script to generate AIND data schema JSON files for visual behavior ophys dataset.

NWB files are streamed from the public S3 bucket (no download required):
    s3://visual-behavior-ophys-data/visual-behavior-ophys
Behavior-only sessions live under ``behavior_sessions/`` and ophys sessions under
``behavior_ophys_experiments/`` (one file per imaging plane). Only small byte ranges
are read, so even multi-GB imaging files are cheap to open for metadata.
"""

import pandas as pd

from pathlib import Path
import h5py
from pynwb import NWBHDF5IO

from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ophys.acquisition_behavior_only import (
    generate_acquisition as generate_acquisition_behavior_only
)
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ophys.acquisition_behavior_ophys import (
    generate_acquisition as generate_acquisition_behavior_ophys
)
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ophys.data_description import generate_data_description
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ophys.subject import fetch_subject_from_aind_metadata_service
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ophys.procedures import fetch_procedures_from_aind_metadata_service
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ophys.instrument import generate_instrument


# Public S3 bucket (HTTPS endpoint) holding the Visual Behavior Ophys NWB files.
S3_BASE_URL = "https://visual-behavior-ophys-data.s3.amazonaws.com/visual-behavior-ophys"


def behavior_session_nwb_url(behavior_session_id: int) -> str:
    """S3 URL for a behavior-only session's NWB file."""
    return f"{S3_BASE_URL}/behavior_sessions/behavior_session_{behavior_session_id}.nwb"


def ophys_experiment_nwb_url(ophys_experiment_id: int) -> str:
    """S3 URL for a single ophys experiment (imaging plane) NWB file."""
    return f"{S3_BASE_URL}/behavior_ophys_experiments/behavior_ophys_experiment_{ophys_experiment_id}.nwb"


def stream_nwb_from_s3(url: str):
    """Stream an NWB file from a public S3/HTTP URL (no download required).

    Uses remfile for block-level caching of the many small byte-range reads that
    opening the HDF5 metadata tree issues, then wraps it with h5py + NWBHDF5IO.

    Parameters
    ----------
    url : str
        HTTPS URL of the NWB file.

    Returns
    -------
    tuple
        ``(nwbfile, io, h5_file, file_handle)``. The caller is responsible for closing
        ``io``, ``h5_file`` and ``file_handle``.
    """
    import remfile

    file_handle = remfile.File(url)
    h5_file = h5py.File(file_handle, "r")
    io = NWBHDF5IO(file=h5_file)
    nwbfile = io.read()
    return nwbfile, io, h5_file, file_handle


def generate_behavior_only_session_metadata(nwbfile, session_info: pd.Series, output_dir: Path):
    """
    Generate AIND data schema JSON files for an (already-opened) behavior-only NWB file.

    Parameters
    ----------
    nwbfile : NWBFile
        The opened behavior-only NWB file
    session_info : pd.Series
        Session metadata row from the behavior session table
    output_dir : Path
        Path to directory to save output JSON files
    """
    # Generate metadata models
    data_description = generate_data_description(nwbfile, session_info)
    subject = fetch_subject_from_aind_metadata_service(nwbfile, session_info)
    acquisition = generate_acquisition_behavior_only(nwbfile, session_info)
    procedures = fetch_procedures_from_aind_metadata_service(nwbfile, session_info)
    instrument = generate_instrument(session_info)
    metadata_models = [data_description, subject, acquisition, procedures, instrument]

    _write_metadata_models(metadata_models, output_dir / data_description.name)


def generate_ophys_session_metadata(
    nwbfiles: list,
    session_infos: list[pd.Series],
    output_dir: Path
):
    """
    Generate AIND data schema JSON files for (already-opened) behavior+ophys NWB file(s).

    For single-plane sessions, nwbfiles contains one file.
    For multiplane sessions, nwbfiles contains one file per imaging plane.

    Parameters
    ----------
    nwbfiles : list[NWBFile]
        List of opened NWB file(s), one per imaging plane.
    session_infos : list[pd.Series]
        List of session metadata rows from the ophys experiment table, one per NWB file.
    output_dir : Path
        Path to directory to save output JSON files
    """
    # Use first file/row for shared metadata
    nwbfile = nwbfiles[0]
    session_info = session_infos[0]

    # Generate metadata models
    data_description = generate_data_description(nwbfile, session_info)
    subject = fetch_subject_from_aind_metadata_service(nwbfile, session_info)
    acquisition = generate_acquisition_behavior_ophys(nwbfiles, session_infos)
    procedures = fetch_procedures_from_aind_metadata_service(nwbfile, session_info)
    instrument = generate_instrument(session_info)
    metadata_models = [data_description, subject, acquisition, procedures, instrument]

    _write_metadata_models(metadata_models, output_dir / data_description.name)


def _write_metadata_models(metadata_models: list, session_output_dir: Path) -> None:
    """Serialize/validate/write each non-None model into the per-session output dir."""
    session_output_dir.mkdir(parents=True, exist_ok=True)
    for model in metadata_models:
        if model is not None:
            serialized = model.model_dump_json()
            deserialized = model.model_validate_json(serialized)
            deserialized.write_standard_file(output_directory=session_output_dir)


def generate_single_session_metadata(
    behavior_session_id: int,
    data_dir: Path,
    results_dir: Path,
) -> None:
    """
    Generate AIND metadata for a single session, streaming the NWB file(s) from S3.

    Session tables are read locally from ``data_dir/visual-behavior-ophys/project_metadata``;
    only the (large) NWB files are streamed from the public S3 bucket.

    Parameters
    ----------
    behavior_session_id : int
        The behavior_session_id to process (from behavior_session_table.csv).
    data_dir : Path
        Data directory containing ``visual-behavior-ophys/project_metadata``.
    results_dir : Path
        Directory to save output metadata JSON files.
    """
    output_dir = results_dir / "visual-behavior-ophys-metadata"
    output_dir.mkdir(parents=True, exist_ok=True)

    cache_dir = data_dir / "visual-behavior-ophys" / "project_metadata"
    behavior_session_table = pd.read_csv(cache_dir / "behavior_session_table.csv")
    ophys_experiment_table = pd.read_csv(cache_dir / "ophys_experiment_table.csv")

    matches = behavior_session_table.query("behavior_session_id == @behavior_session_id")
    if len(matches) != 1:
        raise ValueError(
            f"Expected exactly one row for behavior_session_id {behavior_session_id}, found {len(matches)}"
        )
    row = matches.iloc[0]

    open_handles = []  # (io, h5_file, file_handle) tuples to close in finally
    try:
        if pd.isna(row['ophys_experiment_id']):
            # Behavior-only session
            print(f"Streaming behavior-only session {behavior_session_id} from S3 ...")
            nwbfile, io, h5_file, file_handle = stream_nwb_from_s3(
                behavior_session_nwb_url(behavior_session_id)
            )
            open_handles.append((io, h5_file, file_handle))
            generate_behavior_only_session_metadata(nwbfile, row, output_dir)
        else:
            # Behavior + ophys session: one NWB per imaging plane
            ids_str = str(row['ophys_experiment_id']).strip('[]').strip()
            all_ophys_exp_ids = [int(x.strip()) for x in ids_str.split(',')]

            nwbfiles = []
            session_infos = []
            for ophys_experiment_id in all_ophys_exp_ids:
                exp_info = ophys_experiment_table.query("ophys_experiment_id == @ophys_experiment_id")
                if len(exp_info) != 1:
                    print(f"  Could not find unique ophys_experiment_table entry for {ophys_experiment_id}. Skipping plane.")
                    continue
                print(f"  Streaming ophys experiment {ophys_experiment_id} from S3 ...")
                nwbfile, io, h5_file, file_handle = stream_nwb_from_s3(
                    ophys_experiment_nwb_url(ophys_experiment_id)
                )
                open_handles.append((io, h5_file, file_handle))
                nwbfiles.append(nwbfile)
                session_infos.append(exp_info.iloc[0])

            if not nwbfiles:
                raise RuntimeError(f"No ophys experiment NWB files could be opened for session {behavior_session_id}")

            print(f"  Opened {len(nwbfiles)} plane(s)")
            generate_ophys_session_metadata(nwbfiles, session_infos, output_dir)
    finally:
        for io, h5_file, file_handle in open_handles:
            io.close()
            h5_file.close()
            file_handle.close()

    print(f"Done. Metadata written under {output_dir}")
