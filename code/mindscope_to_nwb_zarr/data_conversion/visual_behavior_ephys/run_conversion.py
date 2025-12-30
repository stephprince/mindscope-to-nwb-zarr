"""Visual Behavior Neuropixels (ephys) NWB HDF5 to Zarr conversion.

This module converts Visual Behavior Neuropixels NWB HDF5 files to Zarr format.
Each session has a base NWB file and multiple probe files that are combined
into a single Zarr output file.

Source data is located in S3 at:
    s3://visual-behavior-neuropixels-data/visual-behavior-neuropixels/

Session structure:
    behavior_ecephys_sessions/{ecephys_session_id}/
        ecephys_session_{ecephys_session_id}.nwb  - Base session file
        probe_{probe_id}.nwb                       - Probe data (one per probe)

    behavior_only_sessions/{behavior_session_id}/
        behavior_session_{behavior_session_id}.nwb - Behavior-only session file
"""

from pathlib import Path
import warnings

from pynwb import NWBFile, NWBHDF5IO, load_namespaces
from hdmf_zarr.nwb import NWBZarrIO
import quilt3 as q3

from mindscope_to_nwb_zarr.data_conversion.conversion_utils import (
    combine_probe_file_info,
    convert_visual_behavior_stimulus_template_to_images,
    add_missing_descriptions,
    fix_vector_index_dtypes,
)

root_dir = Path(__file__).parent.parent.parent.parent
INPUT_FILE_DIR = root_dir.parent / "data" / "visual-behavior-ephys-placeholders"

S3_BUCKET = "s3://visual-behavior-neuropixels-data"
S3_DATA_PATH = "visual-behavior-neuropixels"

# Load NWB extensions used by Visual Behavior Ephys files
load_namespaces(str(root_dir / "ndx-aibs-stimulus-template/ndx-aibs-stimulus-template.namespace.yaml"))
load_namespaces(str(root_dir / "ndx-ellipse-eye-tracking/ndx-ellipse-eye-tracking.namespace.yaml"))
load_namespaces(str(root_dir / "ndx-aibs-ecephys/ndx-aibs-ecephys.namespace.yaml"))


def _open_nwb_hdf5(path: Path, mode: str, manager=None) -> NWBHDF5IO:
    """Open a Visual Behavior Ephys NWB HDF5 file, suppressing cached namespace warnings.

    ndx-aibs-stimulus-template, ndx-ellipse-eye-tracking, and ndx-aibs-ecephys should be
    both cached in the file and loaded via load_namespaces prior to calling this function.
    """
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=(
                r"Ignoring the following cached namespace[\s\S]*"
                r"ndx-aibs-ecephys[\s\S]*"
                r"ndx-aibs-stimulus-template[\s\S]*"
                r"ndx-ellipse-eye-tracking"
            ),
            category=UserWarning
        )
        if manager is not None:
            return NWBHDF5IO(str(path), mode, manager=manager)
        return NWBHDF5IO(str(path), mode)


def add_missing_visual_behavior_ephys_descriptions(nwbfile: NWBFile) -> None:
    """Add missing descriptions to NWB file based on the technical white paper."""

    if nwbfile.experiment_description is None:
        nwbfile.experiment_description = (
            "The Visual Behavior Neuropixels project utilized the "
            "Allen Brain Observatory platform for in vivo Neuropixels "
            "recordings to collect a large-scale, highly standardized "
            "dataset consisting of recordings of neural activity "
            "in mice performing a visually guided task. The Visual "
            "Behavior dataset is built upon a change detection "
            "behavioral task. Briefly, in this go/no-go task, mice "
            "are presented with a continuous series of briefly "
            "presented stimuli and they earn water rewards by correctly "
            "reporting when the identity of the image changes. "
            "This dataset includes recordings using Neuropixels 1.0 "
            "probes. We inserted up to 6 probes simultaneously in "
            "each mouse for up to two consecutive recording days."
        )

    # Add units table description
    if hasattr(nwbfile, 'units') and nwbfile.units is not None:
        nwbfile.units.fields['description'] = (
            "Units identified from spike sorting using Kilosort2. "
            "Note that unlike the data from the Visual Coding Neuropixels pipeline, "
            "for which potential noise units were filtered from the released "
            "dataset, we have elected to return all units for the Visual Behavior "
            "Neuropixels dataset."
        )

    return nwbfile


def download_visual_behavior_ephys_session_files(
    session_id: int,
    session_type: str,
    scratch_dir: Path
) -> tuple[Path, list[Path]]:
    """Download Visual Behavior Ephys NWB files from S3.

    Downloads the base session NWB file and all associated probe files
    for a given session ID.

    Args:
        session_id: The session ID to download files for.
        session_type: Either 'behavior_ephys' or 'behavior'.
        scratch_dir: Directory to download the files to.

    Returns:
        Tuple of (base_file_path, list of probe_file_paths)
    """
    b = q3.Bucket(S3_BUCKET)

    if session_type == 'behavior_ephys':
        session_dir = f"{S3_DATA_PATH}/behavior_ecephys_sessions/{session_id}"
        base_filename = f"ecephys_session_{session_id}.nwb"
    elif session_type == 'behavior':
        session_dir = f"{S3_DATA_PATH}/behavior_only_sessions/{session_id}"
        base_filename = f"behavior_session_{session_id}.nwb"
    else:
        raise ValueError(f"Unknown session_type: {session_type}. Expected 'behavior_ephys' or 'behavior'.")

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
    base_s3_path = f"{session_dir}/{base_filename}"
    if base_s3_path not in nwb_files:
        raise RuntimeError(f"Base session file not found in S3: {base_s3_path}")

    base_download_path = scratch_dir / base_filename
    if not base_download_path.exists():  # TODO remove after testing
        print(f"Downloading base session file to {base_download_path} ...")
        b.fetch(base_s3_path, base_download_path.as_posix())

    # Download probe files (only for behavior_ephys sessions)
    probe_download_paths = []
    if session_type == 'behavior_ephys':
        probe_files = [f for f in nwb_files if 'probe_' in Path(f).name and f != base_s3_path]

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
    """Convert a Visual Behavior Ephys session to Zarr format.

    Combines the base session NWB file with all probe files into a single
    Zarr output file.

    Args:
        base_hdf5_path: Path to the base session NWB HDF5 file.
        probe_hdf5_paths: Paths to probe NWB HDF5 files.
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

            # Convert stimulus templates to Images containers
            print("Converting stimulus templates to Images containers ...")
            convert_visual_behavior_stimulus_template_to_images(nwbfile)

            # Add missing description fields (from technical white paper)
            print("Adding missing descriptions ...")
            add_missing_descriptions(nwbfile)
            add_missing_visual_behavior_ephys_descriptions(nwbfile)

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


def convert_visual_behavior_ephys_hdf5_to_zarr(results_dir: Path, scratch_dir: Path) -> Path | None:
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
        # TODO uncomment after testing
        pass
        # raise RuntimeError(
        #     f"Expected exactly one NWB file in {INPUT_FILE_DIR}, "
        #     f"found {len(input_files)} files."
        # )
    input_file = input_files[-1]

    # Parse session ID and type from filename
    # Patterns: ecephys_session_{session_id}.nwb or behavior_session_{session_id}.nwb
    filename_stem = input_file.stem
    if filename_stem.startswith("ecephys_session_"):
        session_id = int(filename_stem.replace("ecephys_session_", ""))
        session_type = "behavior_ephys"
        zarr_filename = f"ecephys_session_{session_id}.nwb.zarr"
    elif filename_stem.startswith("behavior_session_"):
        session_id = int(filename_stem.replace("behavior_session_", ""))
        session_type = "behavior"
        zarr_filename = f"behavior_session_{session_id}.nwb.zarr"
    else:
        raise ValueError(
            f"Unexpected filename format: {input_file.name}. "
            f"Expected ecephys_session_{{session_id}}.nwb or behavior_session_{{session_id}}.nwb"
        )

    print(f"Processing {session_type} session {session_id} ...")

    # Download session files from S3
    base_file_path, probe_file_paths = download_visual_behavior_ephys_session_files(
        session_id=session_id,
        session_type=session_type,
        scratch_dir=scratch_dir,
    )

    # Determine output path
    zarr_path = results_dir / zarr_filename

    # Convert to Zarr
    convert_session_to_zarr(
        base_hdf5_path=base_file_path,
        probe_hdf5_paths=probe_file_paths,
        zarr_path=zarr_path,
    )

    return zarr_path
