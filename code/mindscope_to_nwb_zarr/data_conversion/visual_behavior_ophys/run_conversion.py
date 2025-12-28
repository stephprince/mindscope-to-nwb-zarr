from pathlib import Path

from hdmf_zarr.nwb import NWBZarrIO
import pandas as pd
from pynwb import NWBFile, load_namespaces
import quilt3 as q3

from mindscope_to_nwb_zarr.data_conversion.conversion_utils import (
    add_missing_descriptions,
    convert_visual_behavior_stimulus_template_to_images,
    open_visual_behavior_nwb_hdf5,
)

root_dir = Path(__file__).parent.parent.parent.parent
INPUT_FILE_DIR = root_dir.parent / "data" / "visual-behavior-ophys"

MULTIPLANE_PROJECT_CODES = {"VisualBehaviorMultiscope", "VisualBehaviorMultiscope4areasx2d"}

# Load updated NWB extensions used by Visual Behavior Ophys files
load_namespaces(str(root_dir / "ndx-aibs-stimulus-template/ndx-aibs-stimulus-template.namespace.yaml"))
load_namespaces(str(root_dir / "ndx-ellipse-eye-tracking/ndx-ellipse-eye-tracking.namespace.yaml"))


def convert_behavior_or_single_plane_nwb_to_zarr(hdf5_path: Path, zarr_path: Path) -> None:
    """Convert behavior or single-plane NWB HDF5 file to Zarr.

    Args:
        hdf5_path: Path to input NWB HDF5 file.
        zarr_path: Path to output Zarr file.
    """
    print(f"Reading NWB file {hdf5_path} ...")
    with open_visual_behavior_nwb_hdf5(hdf5_path, 'r') as read_io:
        read_nwbfile = read_io.read()

        # Set session_id so that naming on DANDI is more similar to original NWB file
        read_nwbfile.session_id = read_nwbfile.identifier

        # Change stimulus_template to Image objects in Images container
        convert_visual_behavior_stimulus_template_to_images(read_nwbfile)

        # Add missing experiment description field (from technical white paper)
        add_missing_descriptions(read_nwbfile)

        print(f"Exporting to Zarr file {zarr_path} ...")
        with NWBZarrIO(str(zarr_path), mode='w') as export_io:
            export_io.export(src_io=read_io, nwbfile=read_nwbfile, write_args=dict(link_data=False))


def combine_multiplane_info(plane_nwbfiles: list[NWBFile], hdf5_paths: list[Path]) -> NWBFile:
    base_nwbfile = plane_nwbfiles[0]

    # Confirm there is only one device in the first file and get reference to it
    assert len(base_nwbfile.devices) == 1, \
        f"Expected 1 device in first file {hdf5_paths[0]}, found {len(base_nwbfile.devices)}: {list(base_nwbfile.devices.keys())}"
    base_device = list(base_nwbfile.devices.values())[0]

    # Rename the imaging plane from the first file
    # ImagingPlanes are stored in base_nwbfile.imaging_planes dict
    assert len(base_nwbfile.imaging_planes) == 1, (
        f"Expected 1 imaging plane in first file {hdf5_paths[0]}, "
        f"found {len(base_nwbfile.imaging_planes)}: {list(base_nwbfile.imaging_planes.keys())}"
    )
    first_imaging_plane = list(base_nwbfile.imaging_planes.values())[0]

    # Rename using the private attribute (NWB containers don't support renaming directly)
    first_imaging_plane._AbstractContainer__name = "imaging_plane_1"
    first_imaging_plane.set_modified()

    # Rename the ophys processing module from the first file
    assert "ophys" in base_nwbfile.processing, \
        f"Expected 'ophys' processing module in first file {hdf5_paths[0]}"
    ophys_module = base_nwbfile.processing["ophys"]

    # Rename using the private attribute
    ophys_module._AbstractContainer__name = "ophys_plane_1"
    ophys_module.set_modified()

    # Rename the metadata entry in the first file
    assert "metadata" in base_nwbfile.lab_meta_data, \
        f"Expected 'metadata' in lab_meta_data of first file {hdf5_paths[0]}"
    metadata_object = base_nwbfile.lab_meta_data["metadata"]
    
    # Rename using the private attribute
    metadata_object._AbstractContainer__name = "metadata_plane_1"
    metadata_object.set_modified()

    # Process each additional NWB file: extract, rename, re-link, and add components
    for i, (additional_nwbfile, file_path) in enumerate(zip(plane_nwbfiles[1:], hdf5_paths[1:]), start=2):
        # Confirm there is only one device in this file
        assert len(additional_nwbfile.devices) == 1, \
            f"Expected 1 device in {file_path}, found {len(additional_nwbfile.devices)}: {list(additional_nwbfile.devices.keys())}"

        # Extract, rename, and re-link the imaging plane to the base device
        if additional_nwbfile.imaging_planes:
            plane_name = list(additional_nwbfile.imaging_planes.keys())[0]
            imaging_plane = additional_nwbfile.imaging_planes[plane_name]
            imaging_plane.reset_parent()  # Detach from original NWB file
            imaging_plane._AbstractContainer__name = f"imaging_plane_{i}"
            # Update the device reference to point to the base file's device
            imaging_plane.fields["device"] = base_device
            # Add to the base NWB file
            base_nwbfile.add_imaging_plane(imaging_plane)

        assert "ophys" in additional_nwbfile.processing, \
            f"Expected 'ophys' processing module in {file_path}"

        # Extract and rename the ophys processing module
        ophys_module = additional_nwbfile.processing["ophys"]
        ophys_module.reset_parent()  # Detach from original NWB file
        ophys_module._AbstractContainer__name = f"ophys_plane_{i}"

        # Update PlaneSegmentation to reference the newly added imaging plane (not the original)
        if "image_segmentation" in ophys_module.data_interfaces:
            image_seg = ophys_module.data_interfaces["image_segmentation"]
            if "cell_specimen_table" in image_seg.plane_segmentations:
                plane_seg = image_seg.plane_segmentations["cell_specimen_table"]
                plane_seg.fields["imaging_plane"] = imaging_plane
            else:
                raise ValueError(f"No cell_specimen_table found in image_segmentation of {file_path}")
        else:
            raise ValueError(f"No image_segmentation found in ophys module of {file_path}")

        # Add to the base NWB file
        base_nwbfile.add_processing_module(ophys_module)

        assert "metadata" in additional_nwbfile.lab_meta_data, \
            f"Expected 'metadata' in lab_meta_data of {file_path}"

        # Extract and rename the metadata object
        metadata_object = additional_nwbfile.lab_meta_data["metadata"]
        metadata_object.reset_parent()  # Detach from original NWB file
        metadata_object._AbstractContainer__name = f"metadata_plane_{i}"

        # Add to the base NWB file
        base_nwbfile.add_lab_meta_data(metadata_object)

    return base_nwbfile


def combine_multiplane_nwb_to_zarr(
    base_hdf5_path: Path,
    additional_hdf5_paths: list[Path],
    zarr_path: Path,
) -> None:
    """Combine multiple single-plane NWB HDF5 files into one multi-plane Zarr file.

    Each input NWB file contains one ImagingPlane, one ophys ProcessingModule,
    and one metadata LabMetaData object. This function:
    - Renames imaging planes to imaging_plane_1, imaging_plane_2, etc.
    - Renames ophys processing modules to ophys_plane_1, ophys_plane_2, etc.
    - Renames metadata objects to metadata_plane_1, metadata_plane_2, etc.
    - Re-links all imaging planes to a single shared device
    - Updates PlaneSegmentation references to point to the correct imaging planes
    - Exports the combined file to Zarr format

    Args:
        base_hdf5_path: Path to the first plane's NWB HDF5 file.
        additional_hdf5_paths: Paths to additional plane NWB HDF5 files.
        zarr_path: Path to output Zarr file.
    """
    all_hdf5_paths = [base_hdf5_path] + additional_hdf5_paths

    print(f"Reading base NWB file {base_hdf5_path} ...")
    base_io = open_visual_behavior_nwb_hdf5(base_hdf5_path, 'r')
    base_nwbfile = base_io.read()

    print(f"Reading {len(additional_hdf5_paths)} additional plane NWB files ...")
    additional_ios = [
        open_visual_behavior_nwb_hdf5(p, 'r', manager=base_io.manager)
        for p in additional_hdf5_paths
    ]
    plane_ios = [base_io] + additional_ios
    plane_nwbfiles = [base_nwbfile] + [io.read() for io in additional_ios]

    print("Combining multiplane info ...")
    combined_nwbfile = combine_multiplane_info(plane_nwbfiles, all_hdf5_paths)

    # Set session_id so that naming on DANDI is more similar to original NWB files
    combined_nwbfile.session_id = combined_nwbfile.identifier

    # Change stimulus_template to Image objects in Images container
    convert_visual_behavior_stimulus_template_to_images(combined_nwbfile)

    # Add missing experiment description field (from technical white paper)
    add_missing_descriptions(combined_nwbfile)

    # Export the combined NWB file to Zarr (link_data=False copies all data)
    print(f"Exporting to Zarr file {zarr_path} ...")
    with NWBZarrIO(str(zarr_path), mode='w') as export_io:
        export_io.export(src_io=base_io, nwbfile=combined_nwbfile, write_args=dict(link_data=False))

    # Close all IOs after export is complete
    for io in plane_ios:
        io.close()

def download_visual_behavior_ophys_file_from_s3(filename: str, scratch_dir: Path) -> Path:
    """Download a Visual Behavior Ophys NWB file from S3.

    Args:
        filename: Name of the NWB file to download.
        scratch_dir: Directory to download the file to.

    Returns:
        Path to the downloaded file.
    """
    b = q3.Bucket("s3://visual-behavior-ophys-data")

    if filename.startswith("behavior_ophys_experiment_"):
        s3_path = f"visual-behavior-ophys/behavior_ophys_experiments/{filename}"
    else:
        raise ValueError(f"Unknown file type: {filename}")

    download_path = (scratch_dir / filename).as_posix()
    print(f"Downloading {filename} from S3 to {download_path} ...")
    b.fetch(s3_path, download_path)
    return download_path


def get_session_info_from_input_file(input_filename: str, behavior_session_table: pd.DataFrame) -> dict:
    """Determine session type and related info from an input NWB filename.

    Args:
        input_filename: Name of the input NWB file (without path).
        behavior_session_table: DataFrame containing behavior session metadata.

    Returns:
        Dict with keys:
            - session_type: "behavior", "single_plane_ophys", "first_multiplane", or "additional_multiplane"
            - behavior_session_id: The behavior session ID
            - ophys_experiment_id: Single ID (for ophys sessions) or None (for behavior-only)
            - all_ophys_experiment_ids: List of all ophys experiment IDs (for multiplane) or None
            - additional_filenames: List of additional filenames to download (for first_multiplane)
    """
    # Parse the input filename to determine session type
    if input_filename.startswith("behavior_session_"):
        # Behavior-only session
        behavior_session_id = int(input_filename.replace("behavior_session_", "").replace(".nwb", ""))
        return {
            "session_type": "behavior",
            "behavior_session_id": behavior_session_id,
            "ophys_experiment_id": None,
            "all_ophys_experiment_ids": None,
            "additional_filenames": [],
        }

    elif input_filename.startswith("behavior_ophys_experiment_"):
        # Ophys session - need to determine if single-plane or multiplane
        ophys_experiment_id = int(input_filename.replace("behavior_ophys_experiment_", "").replace(".nwb", ""))

        # Find the row in behavior_session_table that contains this ophys_experiment_id
        matching_row = None
        for idx, row in behavior_session_table.iterrows():
            if pd.isna(row['ophys_experiment_id']):
                continue

            # Parse the list of ophys_experiment_ids from the string format "[123, 456, 789]"
            ids_str = str(row['ophys_experiment_id']).strip('[]').strip()
            if not ids_str:
                continue
            ophys_exp_ids = [int(x.strip()) for x in ids_str.split(',')]

            if ophys_experiment_id in ophys_exp_ids:
                matching_row = row
                all_ophys_exp_ids = ophys_exp_ids
                break

        if matching_row is None:
            raise RuntimeError(f"Could not find ophys_experiment_id {ophys_experiment_id} in behavior_session_table")

        behavior_session_id = matching_row['behavior_session_id']
        project_code = matching_row['project_code']

        if project_code in MULTIPLANE_PROJECT_CODES:
            # Multiplane session - check if this is the first plane
            if ophys_experiment_id == all_ophys_exp_ids[0]:
                # First plane of multiplane session - need to download additional planes
                additional_filenames = [
                    f"behavior_ophys_experiment_{exp_id}.nwb"
                    for exp_id in all_ophys_exp_ids[1:]  # Skip first, we already have it
                ]
                return {
                    "session_type": "first_multiplane",
                    "behavior_session_id": behavior_session_id,
                    "ophys_experiment_id": ophys_experiment_id,
                    "all_ophys_experiment_ids": all_ophys_exp_ids,
                    "additional_filenames": additional_filenames,
                }
            else:
                # Additional plane of multiplane session - skip
                return {
                    "session_type": "additional_multiplane",
                    "behavior_session_id": behavior_session_id,
                    "ophys_experiment_id": ophys_experiment_id,
                    "all_ophys_experiment_ids": all_ophys_exp_ids,
                    "additional_filenames": [],
                }
        else:
            # Single-plane ophys session
            return {
                "session_type": "single_plane_ophys",
                "behavior_session_id": behavior_session_id,
                "ophys_experiment_id": ophys_experiment_id,
                "all_ophys_experiment_ids": None,
                "additional_filenames": [],
            }

    else:
        raise ValueError(f"Unknown input filename format: {input_filename}")


def convert_visual_behavior_ophys_hdf5_to_zarr(results_dir: Path, scratch_dir: Path) -> Path | None:
    """Convert NWB HDF5 file to Zarr.

    Reads the input NWB file from INPUT_FILE_DIR, determines the session type
    (behavior-only, single-plane ophys, or multiplane ophys), and converts
    accordingly. For multiplane sessions, only the first plane triggers
    conversion (downloading additional planes from S3); additional planes
    are skipped.

    Args:
        results_dir: Directory to save the converted Zarr file.
        scratch_dir: Directory to download additional NWB files to (for multiplane).

    Returns:
        Path to the converted Zarr file, or None if the file was skipped
        (additional plane of multiplane session or no NWB files in the input directory).
    """
    # Confirm there is only one input file in the input directory
    input_files = list(sorted(INPUT_FILE_DIR.glob("*.nwb")))
    if not input_files:
        return None
    elif len(input_files) > 1:
        raise RuntimeError(
            f"Expected exactly one NWB file in {INPUT_FILE_DIR}, "
            f"found {len(input_files)} files."
        )
    input_file = input_files[0]

    # Download behavior session table metadata
    print("Downloading behavior session table metadata from S3 ...")
    b = q3.Bucket("s3://visual-behavior-ophys-data")
    session_metadata_path = "visual-behavior-ophys/project_metadata/behavior_session_table.csv"
    download_path = (scratch_dir / "behavior_session_table.csv").as_posix()
    b.fetch(session_metadata_path, download_path)
    behavior_session_table = pd.read_csv(download_path)

    # Determine session type and get list of additional files to download
    session_info = get_session_info_from_input_file(
        input_filename=input_file.name,
        behavior_session_table=behavior_session_table,
    )

    session_type = session_info["session_type"]

    # Skip additional planes of multiplane sessions
    if session_type == "additional_multiplane":
        print(
            f"Skipping {input_file.name} - additional plane of multiplane session. "
            f"Will be processed with first plane (ophys_experiment_id={session_info['all_ophys_experiment_ids'][0]})."
        )
        return None

    if session_type == "behavior":
        # Behavior-only session
        zarr_filename = input_file.stem + ".zarr"
        zarr_path = results_dir / zarr_filename
        print(f"Converting behavior-only session to {zarr_path} ...")
        convert_behavior_or_single_plane_nwb_to_zarr(input_file, zarr_path)

    elif session_type == "single_plane_ophys":
        # Single-plane ophys session
        zarr_filename = input_file.stem + ".zarr"
        zarr_path = results_dir / zarr_filename
        print(f"Converting single-plane ophys session to {zarr_path} ...")
        convert_behavior_or_single_plane_nwb_to_zarr(input_file, zarr_path)

    elif session_type == "first_multiplane":
        # First plane of multiplane session - download additional planes and combine
        additional_files = [
            download_visual_behavior_ophys_file_from_s3(filename, scratch_dir)
            for filename in session_info["additional_filenames"]
        ]

        behavior_session_id = session_info["behavior_session_id"]
        zarr_filename = f"behavior_session_{behavior_session_id}.zarr"
        zarr_path = results_dir / zarr_filename
        print(f"Converting multiplane session ({len(additional_files) + 1} planes) to {zarr_path} ...")
        combine_multiplane_nwb_to_zarr(input_file, additional_files, zarr_path)

    else:
        raise RuntimeError(f"Unexpected session type: {session_type}")

    return zarr_path