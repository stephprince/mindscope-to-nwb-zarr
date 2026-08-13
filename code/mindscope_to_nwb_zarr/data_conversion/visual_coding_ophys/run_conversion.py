"""Visual Coding 2-photon (ophys) NWB HDF5 to Zarr conversion.

This module converts Visual Coding 2-photon NWB HDF5 files to Zarr format.
Each session has two NWB files: one with metadata and processed 2p data, and
one with raw 2p imaging data. Both are combined into a single Zarr output file.

Source data is downloaded from DANDI Archive:
    dandiset 000728, version 0.240827.1809

Session structure (on DANDI):
    sub-{specimen_id}/
        sub-{specimen_id}_ses-{experiment_id}-{StimX}_behavior+image+ophys.nwb  - Processed data
        sub-{specimen_id}_ses-{experiment_id}-{StimX}_ophys.nwb                 - Raw 2p data

Pipeline input:
    A single zipped AIND metadata folder mounted at data/visual-coding-2p-metadata-only/,
    named for the experiment's data asset. The metadata is unzipped into
    results/<session name>/, the ophys experiment id is read from its data_description.json
    tags (and used to look up the experiment row in ophys_experiments.json), the NWB files
    are fetched from DANDI, and the Zarr store is written inside that folder as
    results/<session name>/<session name>.nwb.zarr.
"""

import json
import re
import zipfile
from pathlib import Path

from hdmf_zarr import ZarrDataIO
from hdmf_zarr.nwb import NWBZarrIO
import pandas as pd
from pynwb import NWBHDF5IO, NWBFile, get_class, load_namespaces
from pynwb.base import ImageReferences
from pynwb.image import GrayscaleImage, Images
import quilt3 as q3

from mindscope_to_nwb_zarr.data_conversion.conversion_utils import H5DatasetDataChunkIterator

root_dir = Path(__file__).parent.parent.parent.parent
# Mount point (on Code Ocean) of the metadata-only data asset: one zip per experiment, each
# named for the experiment's AIND data asset (the zip's stem is the "session name").
METADATA_ZIP_DIR = root_dir.parent / "data" / "visual-coding-2p-metadata-only"

# TEST TOGGLE: each Code Ocean pipeline job mounts exactly one metadata zip. When this is
# set to a zip filename, only the job whose mounted zip matches it does any work; every
# other job is a no-op (writes an empty placeholder, converts nothing). Set to None for
# production, where every job converts its mounted zip.
#
# TESTING: gate the first Code Ocean run to a single session. Replace this PLACEHOLDER with
# the actual first (alphabetically) zip name after regenerating the metadata zips locally
# (run_all_vc_ophys.py --zip); the packaging timestamp in the name changes every run.
TEST_ONLY_ZIP_NAME = "<FIRST_SESSION_ZIP_NAME>.zip"  # PLACEHOLDER — set after regenerating zips

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


def add_order_of_images_to_existing_images_containers(nwbfile: NWBFile) -> None:
    """Add order_of_images to existing Images containers that don't have it.

    Some stimulus templates (e.g., natural_scenes_template, locally_sparse_noise_template)
    are already stored as Images containers but may be missing the order_of_images field.
    This function adds order_of_images based on sorting image names by the numeric suffix.

    For example, images named "NaturalScene1", "NaturalScene2", ..., "NaturalScene117"
    will be ordered numerically (1, 2, ..., 117).

    Args:
        nwbfile: The NWBFile object to modify.
    Returns:
        None. The NWBFile is modified in place.
    """
    for template_name, stimulus_template in nwbfile.stimulus_template.items():
        # Only process Images containers
        if not isinstance(stimulus_template, Images):
            continue

        # Skip if order_of_images already exists
        if stimulus_template.order_of_images is not None:
            continue

        print(f"Adding order_of_images to {template_name} ...")

        # Get all image names and sort by numeric suffix
        image_names = list(stimulus_template.images.keys())

        def extract_number(name: str) -> int:
            """Extract the numeric suffix from an image name."""
            match = re.search(r'(\d+)$', name)
            if match:
                return int(match.group(1))
            return 0

        # Sort image names by their numeric suffix
        sorted_names = sorted(image_names, key=extract_number)

        # Create ordered list of image references
        ordered_images = [stimulus_template.images[name] for name in sorted_names]

        # Add order_of_images to the Images container
        order_of_images = ImageReferences(name="order_of_images", data=ordered_images)
        stimulus_template.order_of_images = order_of_images


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


def _experiment_id_from_metadata(metadata_dir: Path) -> int:
    """Resolve the ophys experiment id from an unzipped AIND metadata folder.

    The ``data_description.json`` written by the metadata pipeline tags each asset with
    ``"ophys_experiment_id: <id>"`` (see ``visual_coding_ophys/data_description.py``). This
    reads that tag to recover the numeric experiment id used to look up the experiment row
    (and thus the DANDI asset paths) for the conversion.
    """
    data_description_path = metadata_dir / "data_description.json"
    if not data_description_path.exists():
        raise RuntimeError(
            f"data_description.json not found in {metadata_dir}; cannot resolve the experiment id."
        )
    with open(data_description_path) as f:
        data_description = json.load(f)
    for tag in data_description.get("tags", []):
        if isinstance(tag, str) and tag.startswith("ophys_experiment_id:"):
            return int(tag.split(":", 1)[1].strip())
    raise RuntimeError(
        f"No 'ophys_experiment_id: <id>' tag found in {data_description_path} "
        f"(tags: {data_description.get('tags')})."
    )


def convert_visual_coding_ophys_hdf5_to_zarr(results_dir: Path, scratch_dir: Path) -> Path | None:
    """Convert NWB HDF5 file to Zarr.

    The pipeline input is a single zipped AIND metadata folder mounted at
    ``METADATA_ZIP_DIR`` (``data/visual-coding-2p-metadata-only`` on Code Ocean), named for
    the experiment's data asset. This unzips that metadata folder into
    ``results_dir/<session name>/``, reads the ophys experiment id from the unzipped
    ``data_description.json`` tags and looks up its row in ``ophys_experiments.json``,
    downloads the processed + raw NWB files from DANDI, modifies the NWBFile object (subject
    id, stimulus template images), adds the raw 2p data as acquisition, and exports to a Zarr
    directory store inside that folder, ``results_dir/<session name>/<session name>.nwb.zarr``.

    Args:
        results_dir: Directory to unzip the metadata into and to write the Zarr store.
        scratch_dir: Directory to download the NWB files to.

    Returns:
        Path to the converted Zarr directory store, or ``None`` if the job is a no-op
        because its mounted zip does not match ``TEST_ONLY_ZIP_NAME`` (in which case an
        empty placeholder file named for the session is written to ``results_dir``).
    """
    # Each pipeline job mounts exactly one metadata zip.
    zip_files = sorted(p for p in METADATA_ZIP_DIR.iterdir() if p.suffix == ".zip")
    if not zip_files:
        raise RuntimeError(f"No metadata zip found in {METADATA_ZIP_DIR}.")
    metadata_zip = zip_files[0]

    # TEST no-op: when a target zip is hardcoded, only that experiment's job does work; every
    # other job writes an empty placeholder file (named for the session) to results and skips.
    if TEST_ONLY_ZIP_NAME is not None and metadata_zip.name != TEST_ONLY_ZIP_NAME:
        results_dir.mkdir(parents=True, exist_ok=True)
        placeholder = results_dir / metadata_zip.stem
        placeholder.touch()
        print(
            f"[TEST_ONLY_ZIP_NAME] Mounted zip {metadata_zip.name} is not the test target "
            f"{TEST_ONLY_ZIP_NAME}; skipping conversion (wrote empty placeholder {placeholder.name})."
        )
        return None

    # The zip stem is the experiment's data asset name; the Zarr store is named after it.
    session_name = metadata_zip.stem
    print(f"Metadata zip: {metadata_zip.name} (session name: {session_name})")

    # Unzip the metadata folder into results/<session name>/.
    metadata_out_dir = results_dir / session_name
    metadata_out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Unzipping metadata into {metadata_out_dir} ...")
    with zipfile.ZipFile(metadata_zip) as zf:
        zf.extractall(metadata_out_dir)

    # Resolve the ophys experiment id from the unzipped data description tags.
    experiment_id = _experiment_id_from_metadata(metadata_out_dir)
    print(f"Ophys experiment ID: {experiment_id}")

    # Download the ophys experiment metadata from S3 and look up this experiment's row by id.
    print("Downloading ophys experiment metadata from S3 ...")
    b = q3.Bucket(S3_BUCKET)
    json_download_path = scratch_dir / "ophys_experiments.json"
    b.fetch(S3_METADATA_PATH, json_download_path.as_posix())
    ophys_experiment_metadata = pd.read_json(json_download_path)
    matches = ophys_experiment_metadata[ophys_experiment_metadata['id'] == experiment_id]
    if len(matches) != 1:
        raise RuntimeError(
            f"Expected exactly one experiment with id {experiment_id} in {S3_METADATA_PATH}, "
            f"found {len(matches)}."
        )
    experiment_row = matches.iloc[0]
    print(f"stimulus_name: {experiment_row['stimulus_name']}")

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

    with NWBHDF5IO(str(processed_file_path), 'r') as processed_io:
        base_nwbfile = processed_io.read()

        # Change subject ID to external donor name from metadata
        new_subject_id = experiment_row['specimen']['donor']['external_donor_name']
        # WARNING: This approach modifies an attribute that should not be 
        # able to be reset. Validation should always be performed afterwards.
        base_nwbfile.subject.fields['subject_id'] = new_subject_id

        # Add ophys experiment metadata to NWB file via extension
        metadata = OphysExperimentMetadata(name="ophys_experiment_metadata", ophys_experiment_metadata=experiment_row.to_json())
        base_nwbfile.add_lab_meta_data(metadata)

        # Change stimulus_template to Image objects in Images container
        convert_natural_movie_template_imageseries_to_images(base_nwbfile)

        # Add order_of_images to existing Images containers that don't have it
        # (e.g., natural_scenes_template, locally_sparse_noise_template)
        add_order_of_images_to_existing_images_containers(base_nwbfile)

        # Add raw 2p data as acquisition
        with NWBHDF5IO(raw_file_path, 'r', manager=processed_io.manager) as raw_io:
            raw_nwbfile = raw_io.read()
            assert 'MotionCorrectedTwoPhotonSeries' in raw_nwbfile.acquisition, (
                "Expected 'MotionCorrectedTwoPhotonSeries' in raw NWB file acquisition"
            )
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
                    # Here we use chunks of (75, 512, X) which results in
                    # about 1500-1700 chunks for a typical raw 2p dataset with
                    # 110,000-120,000 frames.
                    acq_data.fields["data"] = ZarrDataIO(
                        data=data_iterator,
                        chunks=[75, acq_data.data.shape[1], acq_data.data.shape[2]],
                    )
                base_nwbfile.add_acquisition(acq_data)

            # Export to Zarr. The store is named after the zipped session name and written
            # inside the unzipped metadata folder so the metadata and Zarr live together.
            zarr_path = metadata_out_dir / f"{session_name}.nwb.zarr"
            print(f"Exporting to Zarr file {zarr_path} ...")
            with NWBZarrIO(str(zarr_path), mode='w') as export_io:
                export_io.export(src_io=processed_io, nwbfile=base_nwbfile, write_args=dict(link_data=False))

    return zarr_path
