"""Visual Coding Neuropixels (ephys) NWB HDF5 to Zarr conversion.

This module converts Visual Coding Neuropixels NWB HDF5 files to Zarr format.
Each session has a base NWB file and multiple probe LFP files that are combined
into a single Zarr output file.

Source data is located in S3 at:
    s3://allen-brain-observatory/visual-coding-neuropixels/ecephys-cache/

Session structure:
    session_{session_id}/
        session_{session_id}.nwb       - Base session file with units, electrodes, etc.
        probe_{probe_id}_lfp.nwb       - LFP data for each probe (one per probe)
"""

from pathlib import Path
import warnings

from hdmf.build import ObjectMapper
from hdmf_zarr.nwb import NWBZarrIO
import pandas as pd
from pynwb import get_class, load_namespaces, NWBHDF5IO, register_map
import quilt3 as q3

from mindscope_to_nwb_zarr.data_conversion.conversion_utils import (
    combine_probe_file_info,
    add_missing_descriptions,
    fix_vector_index_dtypes,
)

root_dir = Path(__file__).parent.parent.parent.parent
INPUT_FILE_DIR = root_dir.parent / "data" / "visual-coding-ephys-inputs"

S3_BUCKET = "s3://allen-brain-observatory"
S3_ECEPHYS_CACHE_PATH = "visual-coding-neuropixels/ecephys-cache"
S3_SESSIONS_CSV_PATH = f"{S3_ECEPHYS_CACHE_PATH}/sessions.csv"

# Load NWB extensions used by Visual Coding Ephys files
load_namespaces(str(root_dir / "ndx-aibs-ecephys/ndx-aibs-ecephys.namespace.yaml"))
EcephysSpecimen = get_class('EcephysSpecimen', 'ndx-aibs-ecephys')

# The NWB extension ndx-aibs-ecephys 0.2.0 specifies a required "strain" text attribute in
# the new data type EcephysSpecimen which extends the NWB core data type Subject.
# However, since the time that the extension was created, the NWB core Subject data type
# has added an optional "strain" dataset. As a result, when reading the NWB file, the
# EcephysSpecimen "strain" field is not populated, leading to a MissingRequiredBuildWarning.
# To work around this, we use a custom ObjectMapper to construct the EcephysSpecimen object
# by getting the "strain" value from the builder "strain" attribute.
@register_map(EcephysSpecimen)  # TODO does this work?
class CustomEcephysSpecimenMapper(ObjectMapper):
    """Instruct the object mapper for EcephysSpecimen to get strain (str) from builder
    when constructing the object from the EcephysSpecimen builder read from a file.
    """

    @ObjectMapper.constructor_arg("strain")
    def strain_carg(self, builder, manager):
        strain_value = builder.get('strain')
        assert isinstance(strain_value, str)
        return strain_value


def _open_nwb_hdf5(path: Path, mode: str, manager=None) -> NWBHDF5IO:
    """Open a Visual Coding Ephys NWB HDF5 file, suppressing cached namespace warnings.

    ndx-aibs-ecephys should be both cached in the file and loaded via 
    load_namespaces prior to calling this function.
    """
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=(
                r"Ignoring the following cached namespace[\s\S]*"
                r"ndx-aibs-ecephys"
            ),
            category=UserWarning
        )
        if manager is not None:
            return NWBHDF5IO(str(path), mode, manager=manager)
        return NWBHDF5IO(str(path), mode)


def download_visual_coding_ephys_session_files(session_id: int, scratch_dir: Path) -> tuple[Path, list[Path]]:
    """Download Visual Coding Ephys NWB files from S3.

    Downloads the base session NWB file and all associated probe LFP files
    for a given session ID. NWB files for a session are found only in the
    corresponding session directory on S3.

    Args:
        session_id: The session ID to download files for.
        scratch_dir: Directory to download the files to.

    Returns:
        Tuple of (base_file_path, list of probe_file_paths)
    """
    b = q3.Bucket(S3_BUCKET)
    session_dir = f"{S3_ECEPHYS_CACHE_PATH}/session_{session_id}"

    # List all files in the session directory
    print(f"Listing files in {session_dir}/ ...")
    dir_contents = b.ls(f"{session_dir}/")

    if not dir_contents or len(dir_contents) < 2:
        raise RuntimeError(f"No files found in S3 at {session_dir}/")

    # Filter for NWB files
    nwb_files = [
        f['Key'] for f in dir_contents[1]
        if f.get('IsLatest', True) and f['Key'].endswith('.nwb')
    ]

    if not nwb_files:
        raise RuntimeError(f"No NWB files found in S3 at {session_dir}/")

    # Download base session file
    base_filename = f"session_{session_id}.nwb"
    base_s3_path = f"{session_dir}/{base_filename}"
    if base_s3_path not in nwb_files:
        raise RuntimeError(f"Base session file not found in S3: {base_s3_path}")

    base_download_path = scratch_dir / base_filename
    if not base_download_path.exists():  # TODO remove after testing
        print(f"Downloading base session file to {base_download_path} ...")
        b.fetch(base_s3_path, base_download_path.as_posix())

    # Download probe LFP files
    probe_files = [f for f in nwb_files if 'probe_' in Path(f).name and '_lfp.nwb' in f]
    probe_download_paths = []

    for probe_s3_path in sorted(probe_files):
        probe_filename = Path(probe_s3_path).name
        probe_download_path = scratch_dir / probe_filename
        if not probe_download_path.exists():  # TODO remove after testing
            print(f"Downloading probe file to {probe_download_path} ...")
            b.fetch(probe_s3_path, probe_download_path.as_posix())
        probe_download_paths.append(probe_download_path)

    return base_download_path, probe_download_paths


def convert_session_to_zarr(
    base_hdf5_path: Path,
    probe_hdf5_paths: list[Path],
    zarr_path: Path,
) -> None:
    """Convert a Visual Coding Ephys session to Zarr format.

    Combines the base session NWB file with all probe LFP files into a single
    Zarr output file.

    Args:
        base_hdf5_path: Path to the base session NWB HDF5 file.
        probe_hdf5_paths: Paths to probe LFP NWB HDF5 files.
        zarr_path: Path to output Zarr file.
    """
    print(f"Reading base NWB file {base_hdf5_path} ...")
    print(f"  Found {len(probe_hdf5_paths)} probe files:")
    for pf in probe_hdf5_paths:
        print(f"    - {pf.name}")

    with _open_nwb_hdf5(base_hdf5_path, 'r') as read_io:
        nwbfile = read_io.read()

        # Open and read all probe files
        probe_ios = [_open_nwb_hdf5(f, 'r', manager=read_io.manager) for f in probe_hdf5_paths]
        try:
            # Combine LFP and CSD data from each probe file
            for probe_io in probe_ios:
                print(f"Combining probe data from {probe_io.source} ...")
                probe_nwbfile = probe_io.read()
                nwbfile = combine_probe_file_info(nwbfile, probe_nwbfile)

            # TODO - add missing stimulus templates for visual coding dataset
            # load from s3://allen-brain-observatory/visual-coding-neuropixels/ecephys-cache/natural_scene_templates/
            # load from s3://allen-brain-observatory/visual-coding-neuropixels/ecephys-cache/natural_movie_templates/
            # load natural movie shuffled from here:
            # https://community.brain-map.org/t/accessing-frames-for-natural-movie-shuffled-in-neuropixel-data/1010/22

            # Add missing description fields (from technical white paper)
            print("Adding missing descriptions ...")
            add_missing_descriptions(nwbfile)

            # Fix VectorIndex dtypes to be uint64
            print("Fixing VectorIndex dtypes ...")
            fix_vector_index_dtypes(nwbfile)

            # NOTE: The original NWB HDF5 files for Visual Coding - Neuropixels use NWB schema 2.2.0
            # where the "filtering" column (VectorData dataset) of the electrodes table is specified
            # as a float32 dtype. However, the dataset in the file contains string values. This means
            # the original NWB HDF5 file is invalid and the current pynwb validator raises
            # this as a validation error. The earlier version of the validator did not catch this 
            # validation error. In NWB schema 2.4.0, the "filtering" column was 
            # updated to be a variable-length string dtype. When exporting the original NWB file to 
            # Zarr using NWBZarrIO and PyNWB 3.1.2 which uses NWB schema 2.9.0, the "filtering" 
            # column is read as a string dataset and written to Zarr as a string dataset without error
            # or loss of data, so no special handling is needed here.

            # Export to Zarr
            print(f"Exporting to Zarr file {zarr_path} ...")
            with NWBZarrIO(str(zarr_path), mode='w') as export_io:
                export_io.export(src_io=read_io, nwbfile=nwbfile, write_args=dict(link_data=False))
        finally:
            # Close all probe IO objects
            for probe_io in probe_ios:
                probe_io.close()


def convert_visual_coding_ephys_hdf5_to_zarr(results_dir: Path, scratch_dir: Path) -> Path:
    """Convert NWB HDF5 file to Zarr.

    Reads the input file from INPUT_FILE_DIR (a file named with a row index),
    uses that index to look up the session in the sessions.csv table from S3,
    downloads the actual NWB files from S3, and converts to Zarr format.

    Args:
        results_dir: Directory to save the converted Zarr file.
        scratch_dir: Directory to download NWB files to.

    Returns:
        Path to the converted Zarr file.
    """
    # Confirm there is exactly one input file in the input directory
    input_files = list(INPUT_FILE_DIR.iterdir())
    if len(input_files) != 1:
        raise RuntimeError(
            f"Expected exactly one input file in {INPUT_FILE_DIR}, "
            f"found {len(input_files)} files."
        )
    input_file = input_files[0]

    # Parse row index from filename
    row_index = int(input_file.name)
    print(f"Processing row index {row_index} ...")

    # Download sessions.csv from S3
    print("Downloading sessions.csv from S3 ...")
    b = q3.Bucket(S3_BUCKET)
    csv_download_path = scratch_dir / "sessions.csv"
    b.fetch(S3_SESSIONS_CSV_PATH, csv_download_path.as_posix())
    sessions_df = pd.read_csv(csv_download_path)

    # Get the row at the specified index
    if row_index < 0 or row_index >= len(sessions_df):
        raise RuntimeError(
            f"Row index {row_index} out of range. "
            f"Table has {len(sessions_df)} rows (0-{len(sessions_df)-1})."
        )
    session_row = sessions_df.iloc[row_index]
    session_id = int(session_row['id'])
    print(f"Session ID: {session_id}")

    # Download session files from S3
    base_file_path, probe_file_paths = download_visual_coding_ephys_session_files(
        session_id=session_id,
        scratch_dir=scratch_dir,
    )

    # Determine output path
    zarr_filename = f"session_{session_id}.nwb.zarr"
    zarr_path = results_dir / zarr_filename

    # Convert to Zarr
    convert_session_to_zarr(
        base_hdf5_path=base_file_path,
        probe_hdf5_paths=probe_file_paths,
        zarr_path=zarr_path,
    )

    return zarr_path
