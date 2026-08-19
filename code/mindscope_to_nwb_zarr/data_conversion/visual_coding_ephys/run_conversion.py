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

Pipeline input:
    A single zipped AIND metadata folder mounted at
    data/visual-coding-neuropixels-metadata-only/, named for the session's data asset
    (e.g. 386129_2018-06-27_14-07-11_nwb_2026-08-10_15-44-42.zip). The metadata is
    unzipped into results/<session name>/, the session id is read from its
    data_description.json tags to fetch the NWB files from S3, and the Zarr store is
    written inside that folder as results/<session name>/<session name>.nwb.zarr.
"""

import json
import warnings
import zipfile
from pathlib import Path

from hdmf.build import ObjectMapper
from hdmf_zarr.nwb import NWBZarrIO
from pynwb import get_class, load_namespaces, NWBHDF5IO, register_map
import quilt3 as q3

from mindscope_to_nwb_zarr.data_conversion.conversion_utils import (
    combine_probe_file_info,
    add_missing_descriptions,
    fix_vector_index_dtypes,
)
from mindscope_to_nwb_zarr.data_conversion.visual_coding_ephys._units_analysis_metrics import (
    add_electrode_structure_ids,
    add_unit_analysis_metrics,
    add_unit_channel_columns,
    replace_electrode_z_with_ccf_left_right,
    resolve_session_type,
)

root_dir = Path(__file__).parent.parent.parent.parent
# Mount point (on Code Ocean) of the metadata-only data asset: one zip per session, each
# named for the session's AIND data asset (the zip's stem is the "session name").
METADATA_ZIP_DIR = root_dir.parent / "data" / "visual-coding-neuropixels-metadata-only"

# TEST TOGGLE: each Code Ocean pipeline job mounts exactly one session zip. When this is
# set to a zip filename, only the job whose mounted zip matches it does any work; every
# other job is a no-op (nothing downloaded or converted), so the pipeline can be validated
# on a single session without spending compute on all 58. Set to None for production, where
# every job converts its mounted zip.
TEST_ONLY_ZIP_NAME = None

S3_BUCKET = "s3://allen-brain-observatory"
S3_ECEPHYS_CACHE_PATH = "visual-coding-neuropixels/ecephys-cache"

# Load NWB extensions used by Visual Coding Ephys files
load_namespaces(str(root_dir / "ndx-aibs-ecephys/ndx-aibs-ecephys.namespace.yaml"))
EcephysSpecimen = get_class('EcephysSpecimen', 'ndx-aibs-ecephys')

# The source HDF5 files were written with ndx-aibs-ecephys 0.2.0, which specifies "strain" as
# a required text *attribute* on the EcephysSpecimen data type (an extension of the NWB core
# Subject type). Since then, NWB core Subject (>=2.3.0) added an optional "strain" *dataset*,
# and this repository ships/loads ndx-aibs-ecephys 0.3.0 (see code/ndx-aibs-ecephys/CHANGELOG.md),
# which likewise redefines "strain" as a dataset to match core. As a result, when reading with
# the 0.3.0 schema the EcephysSpecimen "strain" field is not auto-populated from the 0.2.0
# attribute (or from the core dataset after an HDF5->Zarr round-trip), leading to a
# MissingRequiredBuildWarning. To work around this, we use a custom ObjectMapper to construct
# the EcephysSpecimen object by reading "strain" from whichever representation the file uses.
def _strain_value_to_str(strain_value):
    """Coerce a builder ``strain`` value to ``str`` (or ``None`` if genuinely absent).

    ``strain`` is a required text *attribute* of the ndx-aibs-ecephys ``EcephysSpecimen``
    type, but the core NWB ``Subject`` later added an optional ``strain`` *dataset*. In the
    original HDF5 files ``builder.get('strain')`` yields the attribute as a ``str``, but
    after an HDF5->Zarr export ``strain`` round-trips as the core dataset, so on read-back
    the builder yields a ``DatasetBuilder`` (whose ``data`` may be ``bytes``). This unwraps
    whichever representation is present; returning ``None`` for an absent value preserves the
    pre-workaround behavior (a non-fatal ``MissingRequiredBuildWarning``) rather than raising.
    """
    if hasattr(strain_value, "data"):  # DatasetBuilder (dataset form, e.g. after Zarr export)
        strain_value = strain_value.data
    if isinstance(strain_value, bytes):
        strain_value = strain_value.decode()
    if strain_value is None:
        return None
    return str(strain_value)


@register_map(EcephysSpecimen)
class CustomEcephysSpecimenMapper(ObjectMapper):
    """Instruct the object mapper for EcephysSpecimen to get strain (str) from builder
    when constructing the object from the EcephysSpecimen builder read from a file.

    The value is read from whichever representation the file uses (the extension's text
    attribute in the original HDF5, or the core Subject's ``strain`` dataset after an
    HDF5->Zarr export); see ``_strain_value_to_str``.
    """

    @ObjectMapper.constructor_arg("strain")
    def strain_carg(self, builder, manager):
        return _strain_value_to_str(builder.get('strain'))


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
    print(f"Downloading base session file to {base_download_path} ...")
    b.fetch(base_s3_path, base_download_path.as_posix())

    # Download probe LFP files
    probe_files = [f for f in nwb_files if 'probe_' in Path(f).name and '_lfp.nwb' in f]
    probe_download_paths = []

    for probe_s3_path in sorted(probe_files):
        probe_filename = Path(probe_s3_path).name
        probe_download_path = scratch_dir / probe_filename
        print(f"Downloading probe file to {probe_download_path} ...")
        b.fetch(probe_s3_path, probe_download_path.as_posix())
        probe_download_paths.append(probe_download_path)

    return base_download_path, probe_download_paths


def convert_session_to_zarr(
    base_hdf5_path: Path,
    probe_hdf5_paths: list[Path],
    zarr_path: Path,
    session_type: str | None = None,
) -> None:
    """Convert a Visual Coding Ephys session to Zarr format.

    Combines the base session NWB file with all probe LFP files into a single
    Zarr output file.

    Args:
        base_hdf5_path: Path to the base session NWB HDF5 file.
        probe_hdf5_paths: Paths to probe LFP NWB HDF5 files.
        zarr_path: Path to output Zarr file.
        session_type: 'brain_observatory_1.1' or 'functional_connectivity'. When provided, the
            AllenSDK per-unit visual-response analysis metrics for that session type are streamed
            from S3 and added to the units table. When None, that step is skipped.
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

            # Add the AllenSDK per-unit visual-response analysis metrics (RF, tuning, running
            # modulation, per-stimulus firing rates, etc.) that are absent from the source NWB,
            # plus per-unit channel-derived columns (CCF coordinates, brain structure and on-probe
            # geometry, joined from the unit's peak channel), the numeric CCF structure id on the
            # electrodes table, and a correction of the electrodes 'z' column (a packaging error
            # left it duplicating 'y' instead of the left-right CCF coordinate). Streamed from the
            # public ecephys-cache CSVs (no AllenSDK dependency); see _units_analysis_metrics.py.
            if session_type is not None:
                print(f"Adding unit analysis metrics ({session_type}) ...")
                add_unit_analysis_metrics(nwbfile, session_type)
            print("Adding per-unit CCF coordinates, brain structure, and probe geometry ...")
            add_unit_channel_columns(nwbfile)
            add_electrode_structure_ids(nwbfile)
            print("Correcting electrodes 'z' with the CCF left-right coordinate ...")
            replace_electrode_z_with_ccf_left_right(nwbfile)

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


def _session_id_from_metadata(metadata_dir: Path) -> int:
    """Resolve the ecephys session id from an unzipped AIND metadata folder.

    The ``data_description.json`` written by the metadata pipeline tags each asset with
    ``"session_id: <id>"`` (see ``visual_coding_ephys/data_description.py``). This reads
    that tag to recover the numeric Visual Coding Neuropixels session id used to fetch the
    NWB files from S3.
    """
    data_description_path = metadata_dir / "data_description.json"
    if not data_description_path.exists():
        raise RuntimeError(
            f"data_description.json not found in {metadata_dir}; cannot resolve the session id."
        )
    with open(data_description_path) as f:
        data_description = json.load(f)
    for tag in data_description.get("tags", []):
        if isinstance(tag, str) and tag.startswith("session_id:"):
            return int(tag.split(":", 1)[1].strip())
    raise RuntimeError(
        f"No 'session_id: <id>' tag found in {data_description_path} "
        f"(tags: {data_description.get('tags')})."
    )


def convert_visual_coding_ephys_hdf5_to_zarr(results_dir: Path, scratch_dir: Path) -> Path | None:
    """Convert a Visual Coding Neuropixels session's NWB HDF5 files to Zarr.

    The pipeline input is a single zipped AIND metadata folder mounted at
    ``METADATA_ZIP_DIR`` (``data/visual-coding-neuropixels-metadata-only`` on Code Ocean),
    named for the session's data asset (e.g.
    ``386129_2018-06-27_14-07-11_nwb_2026-08-10_15-44-42.zip``). This:

    1. unzips that metadata folder into ``results_dir/<session name>/``,
    2. reads the session id from the unzipped ``data_description.json`` tags,
    3. downloads the session's base + probe NWB files from S3, and
    4. exports them to a Zarr directory store inside that folder,
       ``results_dir/<session name>/<session name>.nwb.zarr``.

    Args:
        results_dir: Directory to unzip the metadata into and to write the Zarr store.
        scratch_dir: Directory to download NWB files to.

    Returns:
        Path to the converted Zarr directory store, or ``None`` if the job is a no-op
        because its mounted zip does not match ``TEST_ONLY_ZIP_NAME`` (in which case an
        empty placeholder file named for the session is written to ``results_dir``).
    """
    # Each pipeline job mounts exactly one session zip.
    zip_files = sorted(p for p in METADATA_ZIP_DIR.iterdir() if p.suffix == ".zip")
    if not zip_files:
        raise RuntimeError(f"No metadata zip found in {METADATA_ZIP_DIR}.")
    metadata_zip = zip_files[0]

    # TEST no-op: when a target zip is hardcoded, only that session's job does work; every
    # other job does no download/conversion. It still writes an empty placeholder file
    # (named for the session) to results so the Code Ocean job produces output.
    if TEST_ONLY_ZIP_NAME is not None and metadata_zip.name != TEST_ONLY_ZIP_NAME:
        results_dir.mkdir(parents=True, exist_ok=True)
        placeholder = results_dir / metadata_zip.stem
        placeholder.touch()
        print(
            f"[TEST_ONLY_ZIP_NAME] Mounted zip {metadata_zip.name} is not the test target "
            f"{TEST_ONLY_ZIP_NAME}; skipping conversion (wrote empty placeholder "
            f"{placeholder.name})."
        )
        return None
    # The zip stem is the session's data asset name; the Zarr store is named after it.
    session_name = metadata_zip.stem
    print(f"Metadata zip: {metadata_zip.name} (session name: {session_name})")

    # Unzip the metadata folder into results/<session name>/.
    metadata_out_dir = results_dir / session_name
    metadata_out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Unzipping metadata into {metadata_out_dir} ...")
    with zipfile.ZipFile(metadata_zip) as zf:
        zf.extractall(metadata_out_dir)

    # Resolve the session id from the unzipped data description (tags carry "session_id: <id>").
    session_id = _session_id_from_metadata(metadata_out_dir)
    print(f"Session ID: {session_id}")

    # Resolve the session type (drives which per-unit analysis-metrics file is attached).
    session_type = resolve_session_type(session_id)
    print(f"Session type: {session_type}")

    # Download session files from S3.
    base_file_path, probe_file_paths = download_visual_coding_ephys_session_files(
        session_id=session_id,
        scratch_dir=scratch_dir,
    )

    # Zarr directory store named after the zipped session name, written inside the
    # unzipped metadata folder so each session's metadata and Zarr live together.
    zarr_path = metadata_out_dir / f"{session_name}.nwb.zarr"

    # Convert to Zarr.
    convert_session_to_zarr(
        base_hdf5_path=base_file_path,
        probe_hdf5_paths=probe_file_paths,
        zarr_path=zarr_path,
        session_type=session_type,
    )

    return zarr_path
