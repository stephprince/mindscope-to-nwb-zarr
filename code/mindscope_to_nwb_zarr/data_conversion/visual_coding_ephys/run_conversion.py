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

from pynwb import NWBHDF5IO, load_namespaces
from hdmf_zarr.nwb import NWBZarrIO
import quilt3 as q3

from mindscope_to_nwb_zarr.data_conversion.conversion_utils import (
    combine_probe_file_info,
    add_missing_descriptions,
    fix_vector_index_dtypes,
)

root_dir = Path(__file__).parent.parent.parent.parent
INPUT_FILE_DIR = root_dir.parent / "data" / "visual-coding-ephys-placeholders"

S3_BUCKET = "s3://allen-brain-observatory"
S3_ECEPHYS_CACHE_PATH = "visual-coding-neuropixels/ecephys-cache"

# Load NWB extensions used by Visual Coding Ephys files
load_namespaces(str(root_dir / "ndx-aibs-stimulus-template/ndx-aibs-stimulus-template.namespace.yaml"))
load_namespaces(str(root_dir / "ndx-ellipse-eye-tracking/ndx-ellipse-eye-tracking.namespace.yaml"))
load_namespaces(str(root_dir / "ndx-aibs-ecephys/ndx-aibs-ecephys.namespace.yaml"))


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

        # Set strain to unknown (required field)
        # TODO set to actual strain if known
        nwbfile.subject.strain = "unknown"
        nwbfile.set_modified()

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

            # Add missing experiment description field (from technical white paper)
            print("Adding missing descriptions ...")
            add_missing_descriptions(nwbfile)

            # Fix VectorIndex dtypes to be uint64
            print("Fixing VectorIndex dtypes ...")
            fix_vector_index_dtypes(nwbfile)

            # Export to Zarr
            print(f"Exporting to Zarr file {zarr_path} ...")
            with NWBZarrIO(str(zarr_path), mode='w') as export_io:
                export_io.export(src_io=read_io, nwbfile=nwbfile, write_args=dict(link_data=False))
        finally:
            # Close all probe IO objects
            for probe_io in probe_ios:
                probe_io.close()


def convert_visual_coding_ephys_hdf5_to_zarr(results_dir: Path, scratch_dir: Path) -> Path | None:
    """Convert NWB HDF5 file to Zarr.

    Reads the input placeholder file from INPUT_FILE_DIR, downloads the actual
    NWB files from S3, and converts to Zarr format.

    Args:
        results_dir: Directory to save the converted Zarr file.
        scratch_dir: Directory to download NWB files to.

    Returns:
        Path to the converted Zarr file, or None if no input files found.
    """
    # Confirm there is only one input file in the input directory
    input_files = list(sorted(INPUT_FILE_DIR.glob("*.nwb")))
    if not input_files:
        print(f"No NWB files found in {INPUT_FILE_DIR}")
        return None
    elif len(input_files) > 1:
        # Placeholder NWB HDF5 files for Visual Coding Ophys sessions to indicate which DANDI assets to download for conversion to Zarr format. uncomment after testing
        pass
        # raise RuntimeError(
        #     f"Expected exactly one NWB file in {INPUT_FILE_DIR}, "
        #     f"found {len(input_files)} files."
        # )
    input_file = input_files[0]

    # Parse session ID from filename (pattern: session_{session_id}.nwb)
    if not input_file.stem.startswith("session_"):
        raise ValueError(f"Unexpected filename format: {input_file.name}. Expected session_{{session_id}}.nwb")

    session_id = int(input_file.stem.replace("session_", ""))
    print(f"Processing session {session_id} ...")

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
