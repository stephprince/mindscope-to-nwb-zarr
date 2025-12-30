from pathlib import Path

from pynwb import NWBHDF5IO, NWBFile, get_class, load_namespaces
from pynwb.base import ImageReferences
from pynwb.image import GrayscaleImage, Images
from hdmf_zarr.nwb import NWBZarrIO
from hdmf_zarr import ZarrDataIO
import pandas as pd
import quilt3 as q3

from mindscope_to_nwb_zarr.data_conversion.conversion_utils import H5DatasetDataChunkIterator

root_dir = Path(__file__).parent.parent.parent.parent
INPUT_FILE_DIR = root_dir.parent / "data" / "visual-coding-ophys-inputs"

S3_BUCKET = "s3://allen-brain-observatory"
S3_METADATA_PATH = "visual-coding-2p/ophys_experiments.json"

DANDISET_ID = "000728"
DANDISET_VERSION = "0.240827.1809"

# Mapping from stimulus_name in metadata JSON to stim suffix for DANDI asset path
STIMULUS_NAME_TO_SUFFIX = {
    "three_session_A": "StimA",
    "three_session_B": "StimB",
    "three_session_C": "StimC",
    "three_session_C2": "StimC2",
}

# Load NWB extension used by new Visual Coding Ophys files
load_namespaces(str(root_dir / "ndx-aibs-visual-coding-2p/ndx-aibs-visual-coding-2p.namespace.yaml"))
OphysExperimentMetadata = get_class('OphysExperimentMetadata', 'ndx-aibs-visual-coding-2p')


def get_dandi_asset_paths(experiment_metadata: pd.Series) -> tuple[str, str]:
    """Build DANDI asset paths from experiment metadata.

    Asset paths follow the pattern:
        sub-{specimen_id}/sub-{specimen_id}_ses-{id}_{stim_name}_ophys.nwb (raw)
        sub-{specimen_id}/sub-{specimen_id}_ses-{id}_{stim_name}_behavior+image+ophys.nwb (processed)

    Where stim_name is "StimA", "StimB", "StimC", or "StimC2" based on stimulus_name.

    Args:
        experiment_metadata: A row from the ophys experiment metadata DataFrame.

    Returns:
        Tuple of (processed_asset_path, raw_asset_path)
    """
    specimen_id = experiment_metadata['specimen_id']
    experiment_id = experiment_metadata['id']
    stimulus_name = experiment_metadata['stimulus_name']

    stim_suffix = STIMULUS_NAME_TO_SUFFIX.get(stimulus_name)
    if stim_suffix is None:
        raise ValueError(f"Unknown stimulus_name: {stimulus_name}")

    subject_dir = f"sub-{specimen_id}"
    base_name = f"sub-{specimen_id}_ses-{experiment_id}-{stim_suffix}"

    processed_asset_path = f"{subject_dir}/{base_name}_behavior+image+ophys.nwb"
    raw_asset_path = f"{subject_dir}/{base_name}_ophys.nwb"

    return (processed_asset_path, raw_asset_path)


def download_visual_coding_ophys_files_from_dandi(
    processed_asset_path: str,
    raw_asset_path: str,
    scratch_dir_path: Path
) -> tuple[Path, Path]:
    """Download Visual Coding Ophys NWB files from DANDI.

    Both the NWB file containing metadata and processed 2p data, and
    the NWB file containing raw 2p data are downloaded.

    Args:
        processed_asset_path: DANDI asset path for the processed NWB file.
        raw_asset_path: DANDI asset path for the raw NWB file.
        scratch_dir_path: Directory to download the files to.

    Returns:
        Tuple of Paths: (processed_file_path, raw_file_path)
    """
    from dandi.dandiapi import DandiAPIClient

    processed_file_name = Path(processed_asset_path).name
    raw_file_name = Path(raw_asset_path).name

    with DandiAPIClient() as client:
        dandiset = client.get_dandiset(DANDISET_ID, DANDISET_VERSION)

        # Download processed file
        asset = dandiset.get_asset_by_path(processed_asset_path)
        if not asset:
            raise RuntimeError(
                f"No asset found for processed ophys file {processed_asset_path} "
                f"in DANDI dandiset {DANDISET_ID} version {DANDISET_VERSION}"
            )
        processed_download_path = scratch_dir_path / processed_file_name
        print(f"Downloading processed file to {processed_download_path} ...")
        asset.download(filepath=processed_download_path)

        # Download raw file
        asset = dandiset.get_asset_by_path(raw_asset_path)
        if not asset:
            raise RuntimeError(
                f"No asset found for raw ophys file {raw_asset_path} "
                f"in DANDI dandiset {DANDISET_ID} version {DANDISET_VERSION}"
            )
        raw_download_path = scratch_dir_path / raw_file_name
        print(f"Downloading raw file to {raw_download_path} ...")
        asset.download(filepath=raw_download_path)

    return (processed_download_path, raw_download_path)


def convert_natural_movie_template_imageseries_to_images(nwbfile: NWBFile) -> None:
    """Update the natural movie stimulus template(s) in the NWB file to use an Images container.

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

    # Define the natural movie templates to process
    natural_movie_templates = [
        ("natural_movie_one", "NaturalMovieOne"),
        ("natural_movie_two", "NaturalMovieTwo"),
        ("natural_movie_three", "NaturalMovieThree"),
    ]

    # Confirm at least one natural movie template exists
    found_templates = [
        name
        for name, _ in natural_movie_templates
        if name in nwbfile.stimulus_template
    ]
    assert found_templates, (
        "Expected at least one natural movie stimulus template "
        "(natural_movie_one, natural_movie_two, or natural_movie_three) "
        "in NWBFile"
    )

    for template_name, image_prefix in natural_movie_templates:
        # Check if this natural movie template exists in the file
        if template_name not in nwbfile.stimulus_template:
            continue

        stimulus_template = nwbfile.stimulus_template[template_name]
        assert stimulus_template.__class__.__name__ == "ImageSeries", \
            f"Expected stimulus_template '{template_name}' to be of type ImageSeries"

        # Find the corresponding stimulus presentation IndexSeries
        stimulus_name = f"{template_name}_stimulus"
        assert stimulus_name in nwbfile.stimulus, \
            f"Expected stimulus_presentation '{stimulus_name}' not found in NWBFile"
        stimulus_presentation = nwbfile.stimulus[stimulus_name]
        assert stimulus_presentation.__class__.__name__ == "IndexSeries", \
            f"Expected stimulus_presentation '{stimulus_name}' to be of type IndexSeries"

        # Create new Image objects for each frame in the stimulus template
        # NOTE: This can take about 5 minutes for natural movie one with 900 frames
        images = []
        print(f"Converting {template_name} stimulus template frames to Images container ...")
        for i in range(stimulus_template.data.shape[0]):
            image_frame = GrayscaleImage(
                name=f"{image_prefix}_{i}",
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
        nwbfile.stimulus_template.pop(template_name)

        # Add new stimulus template
        nwbfile.add_stimulus_template(images_container)

        # Update IndexSeries reference
        # WARNING: This approach modifies an attribute that should not be
        # able to be reset. Validation should always be performed afterwards.
        stimulus_presentation.fields['indexed_timeseries'] = None
        stimulus_presentation.fields['indexed_images'] = images_container


def convert_visual_coding_ophys_hdf5_to_zarr(results_dir: Path, scratch_dir: Path) -> Path:
    """Convert NWB HDF5 file to Zarr.

    Reads the input file from INPUT_FILE_DIR (a file named with a row index),
    uses that index to look up the experiment in the ophys experiment metadata,
    downloads the necessary NWB files from DANDI, modifies the NWBFile object
    to update subject ID and stimulus template images, adds raw 2p data as
    acquisition, and exports to Zarr format.

    Args:
        results_dir: Directory to save the converted Zarr file.
        scratch_dir: Directory to download the NWB files to.

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

    # Download ophys experiment metadata from S3
    print("Downloading ophys experiment metadata from S3 ...")
    b = q3.Bucket(S3_BUCKET)
    json_download_path = scratch_dir / "ophys_experiments.json"
    b.fetch(S3_METADATA_PATH, json_download_path.as_posix())
    ophys_experiment_metadata = pd.read_json(json_download_path)

    # Get the row at the specified index
    if row_index < 0 or row_index >= len(ophys_experiment_metadata):
        raise RuntimeError(
            f"Row index {row_index} out of range. "
            f"Metadata has {len(ophys_experiment_metadata)} rows (0-{len(ophys_experiment_metadata)-1})."
        )
    experiment_row = ophys_experiment_metadata.iloc[row_index]
    experiment_id = experiment_row['id']
    print(f"Experiment ID: {experiment_id}, stimulus_name: {experiment_row['stimulus_name']}")

    # Build DANDI asset paths from metadata
    processed_asset_path, raw_asset_path = get_dandi_asset_paths(experiment_row)
    print(f"Processed asset path: {processed_asset_path}")
    print(f"Raw asset path: {raw_asset_path}")

    # Download files from DANDI
    processed_file_path, raw_file_path = download_visual_coding_ophys_files_from_dandi(
        processed_asset_path=processed_asset_path,
        raw_asset_path=raw_asset_path,
        scratch_dir_path=scratch_dir,
    )

    with NWBHDF5IO(processed_file_path, 'r') as processed_io:
        base_nwbfile = processed_io.read()

        # Use the experiment metadata row we already have
        match = ophys_experiment_metadata[ophys_experiment_metadata['id'] == experiment_id]
        if match.empty:
            raise RuntimeError(f"No matching metadata found for experiment_id {experiment_id}")

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
                if acq_data.name == "MotionCorrectedTwoPhotonSeries":
                    # WARNING: This approach modifies an attribute that should not be 
                    # able to be reset. Validation should always be performed afterwards.
                    acq_data.fields["imaging_plane"] = base_nwbfile.get_imaging_plane()

                    # Use an iterator to read raw data in chunks so we don't
                    # have to load the entire dataset into memory at once
                    data_iterator = H5DatasetDataChunkIterator(
                        dataset=acq_data.data,
                        chunk_shape=acq_data.data.chunks,
                        buffer_gb=8,
                    )
                    # Rechunk the raw 2p data to optimize for cloud computing
                    # and also reduce the number of chunks created.
                    # Code Ocean limits the rate of COPY requests per S3 prefix
                    # so we cannot have too many chunks per Zarr array or else
                    # we get a 503 Slow Down error from S3 and a Code Ocean
                    # pipeline task failure.
                    # Here we use chunks of (75, 512, 512) which results in
                    # about 1500-1700 chunks for a typical raw 2p dataset with
                    # 110,000-120,000 frames.
                    assert acq_data.data.shape[1:] == (512, 512), (
                        "Expected raw acquisition data shape to have spatial "
                        f"dimensions (512, 512), found {acq_data.data.shape[1:]}"
                    )
                    acq_data.fields["data"] = ZarrDataIO(
                        data=data_iterator,
                        chunks=[75, 512, 512],
                    )
                base_nwbfile.add_acquisition(acq_data)

            # Export to Zarr
            new_base_filename = processed_file_path.stem.replace(old_subject_id, new_subject_id)
            zarr_path = results_dir / "visual-coding-ophys" / f"{new_base_filename}.zarr"
            print(f"Exporting to Zarr file {zarr_path} ...")
            with NWBZarrIO(str(zarr_path), mode='w') as export_io:
                export_io.export(src_io=processed_io, nwbfile=base_nwbfile, write_args=dict(link_data=False))

    return zarr_path
