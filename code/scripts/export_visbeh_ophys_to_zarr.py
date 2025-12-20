from pathlib import Path

from pynwb import NWBHDF5IO
from hdmf_zarr.nwb import NWBZarrIO


# # Single-plane NWB files from a multi-plane ophys session (one file per imaging plane)
# file_paths = [
#     "data/sub-457841_ses-20190920T095938_obj-1almo5m_image+ophys.nwb",
#     "data/sub-457841_ses-20190920T095938_obj-1gz4qr1_image+ophys.nwb",
#     "data/sub-457841_ses-20190920T095938_obj-1wzpvaq_image+ophys.nwb",
#     "data/sub-457841_ses-20190920T095938_obj-8gw2cr_image+ophys.nwb",
#     "data/sub-457841_ses-20190920T095938_obj-15smnjp_image+ophys.nwb",
#     "data/sub-457841_ses-20190920T095938_obj-fvfzhd_image+ophys.nwb",
#     "data/sub-457841_ses-20190920T095938_obj-ohjygt_image+ophys.nwb",
# ]

# # Output path for the combined multi-plane Zarr file
# zarr_filename = "data/sub-457841_ses-20190920T095938_combined_image+ophys.nwb.zarr"


def convert_behavior_or_single_plane_nwb_to_zarr(hdf5_path: Path, zarr_path: Path):
    """Convert behavior or single-plane NWB HDF5 file to Zarr."""
    with NWBHDF5IO(str(hdf5_path), 'r') as read_io:
        with NWBZarrIO(str(zarr_path), mode='w') as export_io:
            export_io.export(src_io=read_io, write_args=dict(link_data=False))


def combine_multiplane_nwb_to_zarr(hdf5_paths: list[Path], zarr_filename: Path):
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
    # Read the first NWB file - this will be our base file
    with NWBHDF5IO(str(hdf5_paths[0]), 'r') as first_io:
        nwbfile = first_io.read()

        # Confirm there is only one device in the first file and get reference to it
        assert len(nwbfile.devices) == 1, \
            f"Expected 1 device in first file {hdf5_paths[0]}, found {len(nwbfile.devices)}: {list(nwbfile.devices.keys())}"
        base_device = list(nwbfile.devices.values())[0]

        # Rename the imaging plane from the first file
        # ImagingPlanes are stored in nwbfile.imaging_planes dict
        assert len(nwbfile.imaging_planes) == 1, \
            f"Expected 1 imaging plane in first file {hdf5_paths[0]}, found {len(nwbfile.imaging_planes)}: {list(nwbfile.imaging_planes.keys())}"
        first_imaging_plane = list(nwbfile.imaging_planes.values())[0]

        # Rename using the private attribute (NWB containers don't support renaming directly)
        first_imaging_plane._AbstractContainer__name = "imaging_plane_1"
        first_imaging_plane.set_modified()

        # Rename the ophys processing module from the first file
        assert "ophys" in nwbfile.processing, \
            f"Expected 'ophys' processing module in first file {hdf5_paths[0]}"
        ophys_module = nwbfile.processing["ophys"]

        # Rename using the private attribute
        ophys_module._AbstractContainer__name = "ophys_plane_1"
        ophys_module.set_modified()

        # Rename the metadata entry in the first file
        assert "metadata" in nwbfile.lab_meta_data, \
            f"Expected 'metadata' in lab_meta_data of first file {hdf5_paths[0]}"
        metadata_object = nwbfile.lab_meta_data["metadata"]
        
        # Rename using the private attribute
        metadata_object._AbstractContainer__name = "metadata_plane_1"
        metadata_object.set_modified()

        # Keep all additional IOs open until export is done (required for data access during export)
        additional_ios = []

        # Process each additional NWB file: extract, rename, re-link, and add components
        for i, file_path in enumerate(hdf5_paths[1:], start=2):
            # Make sure to use the same manager as the first IO so that extensions are mapped correctly
            additional_io = NWBHDF5IO(str(file_path), 'r', manager=first_io.manager)
            additional_ios.append(additional_io)
            additional_nwbfile = additional_io.read()

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
                nwbfile.add_imaging_plane(imaging_plane)

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
            nwbfile.add_processing_module(ophys_module)

            assert "metadata" in additional_nwbfile.lab_meta_data, \
                f"Expected 'metadata' in lab_meta_data of {file_path}"

            # Extract and rename the metadata object
            metadata_object = additional_nwbfile.lab_meta_data["metadata"]
            metadata_object.reset_parent()  # Detach from original NWB file
            metadata_object._AbstractContainer__name = f"metadata_plane_{i}"

            # Add to the base NWB file
            nwbfile.add_lab_meta_data(metadata_object)

        # Export the combined NWB file to Zarr (link_data=False copies all data)
        with NWBZarrIO(str(zarr_filename), mode='w') as export_io:
            export_io.export(src_io=first_io, write_args=dict(link_data=False))

        # Close all additional IOs after export is complete
        for additional_io in additional_ios:
            additional_io.close()
