from pathlib import Path
import warnings

from hdmf_zarr.nwb import NWBZarrIO
import pandas as pd
from pynwb import NWBHDF5IO, NWBFile


def _open_nwbhdf5(path: Path, mode: str = 'r', manager=None) -> NWBHDF5IO:
    """Open an NWB HDF5 file, suppressing cached namespace warnings."""
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



def iterate_visual_behavior_ophys_sessions(data_dir: Path):
    """Iterate through behavior_session_table.csv and yield NWB file paths.

    NWB files follow naming patterns:
    - Behavior only: behavior_session_{behavior_session_id}.nwb
    - Behavior-ophys: behavior_ophys_experiment_{ophys_experiment_id}.nwb

    For multi-plane behavior-ophys sessions (Multiscope projects),
    yields a single session_info dict with nwb_path as a list of paths.
    For single-plane sessions, nwb_path is a single Path object.
    """
    MULTIPLANE_PROJECT_CODES = {"VisualBehaviorMultiscope", "VisualBehaviorMultiscope4areasx2d"}

    VISBEH_OPHYS_BEHAVIOR_DATA_DIR = data_dir / "behavior_sessions"
    VISBEH_OPHYS_BEHAVIOR_OPHYS_DATA_DIR = data_dir / "behavior_ophys_experiments"

    VISBEH_OPHYS_METADATA_TABLES_DIR = data_dir / "project_metadata"
    assert VISBEH_OPHYS_METADATA_TABLES_DIR.exists(), \
        f"Visual behavior ophys project metadata tables directory does not exist: {VISBEH_OPHYS_METADATA_TABLES_DIR}"

    csv_path = VISBEH_OPHYS_METADATA_TABLES_DIR / "behavior_session_table.csv"
    df = pd.read_csv(csv_path)

    for idx, row in df.iterrows():
        behavior_session_id = row['behavior_session_id']
        ophys_experiment_id = row['ophys_experiment_id']
        project_code = row['project_code']

        if pd.isna(ophys_experiment_id):
            # No ophys session - behavior only
            data_dir = VISBEH_OPHYS_BEHAVIOR_DATA_DIR
            session_type = "behavior"
            nwb_filename = f"behavior_session_{behavior_session_id}.nwb"
            nwb_path = data_dir / nwb_filename

            yield {
                'behavior_session_id': behavior_session_id,
                'ophys_experiment_id': None,
                'session_type': session_type,
                'nwb_path': nwb_path,
            }
        else:
            # Has ophys session - parse the list of ophys_experiment_ids
            data_dir = VISBEH_OPHYS_BEHAVIOR_OPHYS_DATA_DIR
            session_type = "behavior_ophys"

            # ophys_experiment_id is stored as a string like "[123, 456, 789]"
            # Parse by stripping brackets and splitting on commas
            ids_str = ophys_experiment_id.strip('[]').strip()
            if not ids_str:
                warnings.warn(f"behavior_session_id {behavior_session_id} has empty "
                              f"ophys_experiment_id list, skipping")
                continue
            ophys_exp_ids = [int(x.strip()) for x in ids_str.split(',')]

            if project_code in MULTIPLANE_PROJECT_CODES:
                # Multi-plane session: store all NWB paths in a list
                nwb_paths = [
                    data_dir / f"behavior_ophys_experiment_{ophys_exp_id}.nwb"
                    for ophys_exp_id in ophys_exp_ids
                ]
                yield {
                    'behavior_session_id': behavior_session_id,
                    'ophys_experiment_id': ophys_exp_ids,
                    'session_type': session_type,
                    'nwb_path': nwb_paths,
                }
            else:
                # Single-plane session
                ophys_exp_id = ophys_exp_ids[0]
                nwb_filename = f"behavior_ophys_experiment_{ophys_exp_id}.nwb"
                nwb_path = data_dir / nwb_filename

                yield {
                    'behavior_session_id': behavior_session_id,
                    'ophys_experiment_id': ophys_exp_id,
                    'session_type': session_type,
                    'nwb_path': nwb_path,
                }


def convert_behavior_or_single_plane_nwb_to_zarr(hdf5_path: Path, zarr_path: Path):
    """Convert behavior or single-plane NWB HDF5 file to Zarr."""
    with _open_nwbhdf5(hdf5_path, 'r') as read_io:
        read_nwbfile = read_io.read()
        # Set session_id so that naming on DANDI is more similar to original NWB file
        read_nwbfile.session_id = read_nwbfile.identifier
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


def combine_multiplane_nwb_to_zarr(hdf5_paths: list[Path], zarr_path: Path):
    """Combine multiple single-plane NWB HDF5 files into one multi-plane Zarr file.

    Each input NWB file contains one ImagingPlane, one ophys ProcessingModule,
    and one metadata LabMetaData object. This function:
    - Renames imaging planes to imaging_plane_1, imaging_plane_2, etc.
    - Renames ophys processing modules to ophys_plane_1, ophys_plane_2, etc.
    - Renames metadata objects to metadata_plane_1, metadata_plane_2, etc.
    - Re-links all imaging planes to a single shared device
    - Updates PlaneSegmentation references to point to the correct imaging planes
    - Exports the combined file to Zarr format
    """
    base_io = _open_nwbhdf5(hdf5_paths[0], 'r')
    base_nwbfile = base_io.read()
    plane_ios = [base_io] + [_open_nwbhdf5(p, 'r', manager=base_io.manager) for p in hdf5_paths[1:]]
    plane_nwbfiles = [base_nwbfile] + [io.read() for io in plane_ios[1:]]

    combined_nwbfile = combine_multiplane_info(plane_nwbfiles, hdf5_paths)

    # Set session_id so that naming on DANDI is more similar to original NWB files
    combined_nwbfile.session_id = combined_nwbfile.identifier

    # Export the combined NWB file to Zarr (link_data=False copies all data)
    with NWBZarrIO(str(zarr_path), mode='w') as export_io:
        export_io.export(src_io=base_io, nwbfile=combined_nwbfile, write_args=dict(link_data=False))

    # Close all additional IOs after export is complete
    for io in plane_ios:
        io.close()
