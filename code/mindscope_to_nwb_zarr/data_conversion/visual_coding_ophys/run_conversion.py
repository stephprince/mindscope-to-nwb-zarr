from pathlib import Path

from pynwb import NWBHDF5IO, NWBFile, get_class, load_namespaces
from pynwb.base import ImageReferences
from pynwb.image import GrayscaleImage, Images
from hdmf_zarr.nwb import NWBZarrIO
import pandas as pd

# load modified extensions before impoting local modules that use them
root_dir = Path(__file__).parent.parent.parent.parent
load_namespaces(str(root_dir / "ndx-aibs-visual-coding-2p/ndx-aibs-visual-coding-2p.namespace.yaml"))
OphysExperimentMetadata = get_class('OphysExperimentMetadata', 'ndx-aibs-visual-coding-2p')

from ..conversion_utils import H5DatasetDataChunkIterator


# OPHYS_EXPERIMENT_METADATA_FILE = root_dir.parent / "data" / "allen-brain-observatory" / "visual-coding-2p" / "ophys_experiments.json"
OPHYS_EXPERIMENT_METADATA_FILE = root_dir.parent / "data" / "visual_coding_2p_ophys_experiments.json"
INPUT_FILE_DIR = root_dir.parent / "data" / "visual-coding-ophys" 
DANDISET_ID = "000728"
DANDISET_VERSION = "0.240827.1809"


def download_visual_coding_ophys_files_from_dandi(processed_file_name: str, scratch_dir_path: Path) -> tuple[Path, Path]:
    """Download Visual Coding Ophys NWB files from DANDI.
    
    Both the NWB file containing metadata and processed 2p data, and
    the NWB file containing raw 2p data are downloaded.

    Args:
        processed_file_name: Base name of the processed NWB file to download.
        scratch_dir_path: Directory to download the files to.

    Returns:
        Tuple of Paths: (processed_file_path, raw_file_path)
    """
    # Parse subject ID directory name from file name
    subject_dir = processed_file_name.split('_')[0]

    # Get raw 2p file name
    raw_file_name = processed_file_name.replace("behavior+image+ophys", "ophys")

    from dandi.dandiapi import DandiAPIClient

    with DandiAPIClient() as client:
        dandiset = client.get_dandiset(DANDISET_ID, DANDISET_VERSION)

        # Download processed file
        asset = dandiset.get_asset_by_path(f"{subject_dir}/{processed_file_name}")
        if not asset:
            raise RuntimeError(f"No asset found for processed ophys file {processed_file_name} in DANDI dandiset {DANDISET_ID} version {DANDISET_VERSION}")
        processed_download_path = scratch_dir_path / processed_file_name
        print(f"Downloading processed file to {processed_download_path} ...")
        asset.download(filepath=processed_download_path)

        # Download raw file
        asset = dandiset.get_asset_by_path(f"{subject_dir}/{raw_file_name}")
        if not asset:
            raise RuntimeError(f"No asset found for raw ophys file {raw_file_name} in DANDI dandiset {DANDISET_ID} version {DANDISET_VERSION}")
        raw_download_path = scratch_dir_path / raw_file_name
        print(f"Downloading raw file to {raw_download_path} ...")
        asset.download(filepath=raw_download_path)

    return (processed_download_path, raw_download_path)


def convert_natural_movie_template_imageseries_to_images(nwbfile: NWBFile) -> None:
    """Update the natural movie stimulus template in the NWB file to use an Images container.

    In the original HDF5 versions of the data, stimulus template images, e.g., four
    gratings or eight natural images, were stored in an NWB ImageSeries object where
    the timestamps are NaN or starting time and sampling rate are NaN. 
    In the /stimulus/presentation group, a separate IndexSeries
    object represents the times at which each image in the ImageSeries is displayed.
    This approach of linking an IndexSeries to an ImageSeries with NaN timestamps is
    deprecated. This function reorganizes the stimulus templates by changing the
    ImageSeries to an ordered set of Image objects in an Images container, and
    changing the IndexSeries to link to this Images container.

    Args:
        nwbfile: The NWBFile object to modify.
    Returns:
        None. The NWBFile is modified in place.
    """

    # Find the natural movie stimulus template ImageSeries
    assert "natural_movie_one" in nwbfile.stimulus_template, \
        "Expected stimulus_template 'natural_movie_one' not found in NWBFile"
    stimulus_template = nwbfile.stimulus_template["natural_movie_one"]
    assert stimulus_template.__class__.__name__ == "ImageSeries", \
        "Expected stimulus_template 'natural_movie_one' to be of type ImageSeries"
    
    # Find the corresponding stimulus presentation IndexSeries
    assert "natural_movie_one_stimulus" in nwbfile.stimulus, \
        "Expected stimulus_presentation 'natural_movie_one_stimulus' not found in NWBFile"
    stimulus_presentation = nwbfile.stimulus["natural_movie_one_stimulus"]
    assert stimulus_presentation.__class__.__name__ == "IndexSeries", \
        "Expected stimulus_presentation 'natural_movie_one_stimulus' to be of type IndexSeries"

    # Create new Image objects for each frame in the stimulus template
    # NOTE: This can take about 5 minutes for natural movie one with 900 frames
    images = []
    print("Converting natural movie stimulus template frames to Images container ...")
    for i in range(stimulus_template.data.shape[0]):
        image_frame = GrayscaleImage(
            name=f"NaturalMovieOne_{i}",
            data=stimulus_template.data[i],
            description="A single frame of a natural movie presented to the subject.",
        )
        images.append(image_frame)

    # Create new Images container
    images_container = Images(
        name=stimulus_template.name,
        description=stimulus_template.description,
        images=images,
        order_of_images=ImageReferences(name="order_of_images", data=images),
    )

    # Remove old stimulus template
    nwbfile.stimulus_template.pop("natural_movie_one")

    # Add new stimulus template
    nwbfile.add_stimulus_template(images_container)

    # Update IndexSeries reference
    # WARNING: This approach modifies an attribute that should not be 
    # able to be reset. Validation should always be performed afterwards.
    stimulus_presentation.fields['indexed_timeseries'] = None
    stimulus_presentation.fields['indexed_images'] = images_container


def convert_visual_coding_ophys_hdf5_to_zarr(results_dir: Path, scratch_dir: Path) -> None:
    """Convert NWB HDF5 file to Zarr.

    Downloads the necessary NWB files from DANDI, modifies the NWBFile object
    to update subject ID and stimulus template images, adds raw 2p data as
    acquisition, and exports to Zarr format.
    
    Args:
        results_dir: Directory to save the converted Zarr file.
        scratch_dir: Directory to download the NWB files to.
    """
    ophys_experiment_metadata = pd.read_json(OPHYS_EXPERIMENT_METADATA_FILE)

    # Confirm there is only one placeholder file in the input directory
    placeholder_files = list(INPUT_FILE_DIR.glob("*.nwb"))
    if len(placeholder_files) != 1:
        raise RuntimeError(f"Expected exactly one NWB placeholder file in {INPUT_FILE_DIR}, found {len(placeholder_files)} files.")
    processed_file_name = placeholder_files[0]

    processed_file_path, raw_file_path = download_visual_coding_ophys_files_from_dandi(
        processed_file_name=processed_file_name.name, 
        scratch_dir_path=scratch_dir
    )

    with NWBHDF5IO(processed_file_path, 'r') as processed_io:
        base_nwbfile = processed_io.read()

        # Get experiment metadata for this file
        experiment_id = base_nwbfile.session_id.split('-')[0]
        match = ophys_experiment_metadata[ophys_experiment_metadata['id'] == int(experiment_id)]
        if match.empty:
            raise RuntimeError(f"No matching metadata found for experiment_id {experiment_id}, file {processed_file_path}")

        # Change subject ID to external donor name from metadata
        old_subject_id = base_nwbfile.subject.subject_id
        new_subject_id = match['specimen'].item()['donor']['external_donor_name']
        # WARNING: This approach modifies an attribute that should not be 
        # able to be reset. Validation should always be performed afterwards.
        base_nwbfile.subject.fields['subject_id'] = new_subject_id

        # Add ophys experiment metadata to NWB file via extension
        metadata = OphysExperimentMetadata(name="ophys_experiment_metadata", ophys_experiment_metadata=match.to_json())
        base_nwbfile.add_lab_meta_data(metadata)

        # Change stimulus_template to Image objects in Images container
        convert_natural_movie_template_imageseries_to_images(base_nwbfile)

        # Add raw 2p data as acquisition
        with NWBHDF5IO(raw_file_path, 'r', manager=processed_io.manager) as raw_io:
            raw_nwbfile = raw_io.read()
            for acq_data in raw_nwbfile.acquisition.values():
                acq_data.reset_parent()
                # WARNING: This approach modifies an attribute that should not be 
                # able to be reset. Validation should always be performed afterwards.
                acq_data.fields["imaging_plane"] = base_nwbfile.get_imaging_plane()
                acq_data.fields["data"] = H5DatasetDataChunkIterator(
                    dataset=acq_data.data,
                    chunk_shape=acq_data.data.chunks,
                    buffer_gb=8,
                )
                base_nwbfile.add_acquisition(acq_data)

            # Export to Zarr
            new_base_filename = processed_file_path.stem.replace(old_subject_id, new_subject_id)
            zarr_path = results_dir / "visual-coding-ophys" / f"{new_base_filename}.zarr"
            print(f"Exporting to Zarr file {zarr_path} ...")
            with NWBZarrIO(str(zarr_path), mode='w') as export_io:
                export_io.export(src_io=processed_io, nwbfile=base_nwbfile, write_args=dict(link_data=False))

            return zarr_path
