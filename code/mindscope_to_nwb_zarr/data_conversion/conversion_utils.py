from pathlib import Path
import traceback
from typing import Any, Iterable, Optional
import warnings

from numcodecs import GZip
import numpy as np
from hdmf.common.table import VectorIndex
from hdmf.data_utils import GenericDataChunkIterator
from hdmf_zarr import ZarrDataIO
from hdmf_zarr.nwb import NWBZarrIO
from nwbinspector import (
    inspect_nwbfile_object,
    format_messages,
    save_report,
    load_config,
    Importance,
    InspectorMessage
)
from pynwb import NWBFile, validate, get_class, NWBHDF5IO
from pynwb.base import ImageReferences
from pynwb.ecephys import LFP
from pynwb.image import Images, GrayscaleImage, IndexSeries


class H5DatasetDataChunkIterator(GenericDataChunkIterator):
    """A data chunk iterator that reads chunks over the 0th dimension of an HDF5 dataset."""

    def __init__(self, dataset: Any, **kwargs: Any) -> None:
        self.dataset = dataset
        super().__init__(**kwargs)

    def _get_data(self, selection: Any) -> Any:
        return self.dataset[selection]

    def _get_maxshape(self) -> tuple[int, ...]:
        return self.dataset.shape

    def _get_dtype(self) -> Any:
        return self.dataset.dtype


def convert_visual_behavior_stimulus_template_to_images(nwbfile: NWBFile) -> None:
    """Convert Visual Behavior stimulus_template from StimulusTemplate to Images container.

    In the original HDF5 versions of Visual Behavior data, stimulus template images
    (e.g., gratings or natural images) were stored in a StimulusTemplate object.
    In the /stimulus/presentation group, a separate IndexSeries object represented
    the times at which each image in the StimulusTemplate was displayed.
    This approach of linking an IndexSeries to a StimulusTemplate is deprecated in NWB.
    This function reorganizes the stimulus templates by changing the StimulusTemplate
    to an ordered set of Image objects in an Images container, and changing the
    IndexSeries to link to this Images container.

    Visual Behavior Ephys files use StimulusTemplate for stimulus templates, e.g.,
    "gratings", "Natural_Images_Lum_Matched_set_ophys_G_2019.05.26" that may or may not have
    matching presentation series.

    Visual Behavior 2p files use StimulusTemplate for stimulus templates, e.g.,
    "grating", "Natural_Images_Lum_Matched_set_training_2017.07.14" with additional
    presentation series, e.g., "spontaneous_stimulus" (TimeIntervals),
    "static_gratings" (TimeIntervals).

    Args:
        nwbfile: The NWBFile object to convert.
    Returns:
        None, the NWBFile object is modified in place.
    """
    try:
        WarpedStimulusTemplateImage = get_class("WarpedStimulusTemplateImage", "ndx-aibs-stimulus-template")
    except KeyError:
        raise RuntimeError(
            "The ndx-aibs-stimulus-template extension was not found. Please first load the namespace "
            "with 'load_namespaces(\"ndx-aibs-stimulus-template/ndx-aibs-stimulus-template.namespace.yaml\")'"
        )

    # Find the stimulus templates
    if not nwbfile.stimulus_template:
        warnings.warn("NWBFile has no stimulus_template field populated. Skipping conversion.")
        return nwbfile

    original_stimulus_keys = list(nwbfile.stimulus_template.keys())
    new_stimulus_templates = {}
    for k in original_stimulus_keys:
        print(f"Converting stimulus template {k} to Images container ...")

        stimulus_template = nwbfile.stimulus_template[k]
        assert stimulus_template.__class__.__name__ == "StimulusTemplate", \
            f"Expected stimulus template '{k}' to be of type StimulusTemplate"

        # Validate data shapes match
        data_shape = stimulus_template.data.shape
        unwarped_shape = stimulus_template.unwarped.shape
        num_control_descriptions = len(stimulus_template.control_description)
        if data_shape != unwarped_shape:
            raise ValueError(
                f"Stimulus template '{k}' data shape mismatch: "
                f"data.shape={data_shape}, unwarped.shape={unwarped_shape}"
            )
        if num_control_descriptions != data_shape[0]:
            raise ValueError(
                f"Stimulus template '{k}' has {data_shape[0]} images but "
                f"{num_control_descriptions} control descriptions"
            )
        
        # Validate we have 3D data (num_images, height, width for GrayscaleImage)
        if len(data_shape) != 3:
            raise ValueError(
                f"Stimulus template '{k}' data should be 3D (num_images, height, width), "
                f"but got shape {data_shape}"
            )
        image_data = stimulus_template.data[:]  # Shape should be (num_images, height, width)
        image_data_unwarped = stimulus_template.unwarped[:]

        # Adapt description
        if stimulus_template.description is not None and stimulus_template.description != "no description":
            description = stimulus_template.description
        else:
            description = "Visual stimuli images shown to the subject."

        # Create new image objects
        all_images_unwarped = []
        all_images = []
        for i in range(image_data.shape[0]):
            unwarped_image = GrayscaleImage(
                name=stimulus_template.control_description[i],
                data=image_data_unwarped[i],
                description=f"Unwarped stimulus template: {stimulus_template.control[i]}",
            )
            warped_image = WarpedStimulusTemplateImage(
                name=stimulus_template.control_description[i],
                data=image_data[i],
                description=f"Warped stimulus template: {stimulus_template.control[i]}",
                unwarped=unwarped_image,
            )
            all_images_unwarped.append(unwarped_image)
            all_images.append(warped_image)

        # Create and add Images containers
        all_images_container = Images(
            name=stimulus_template.name,
            description=description,
            images=all_images,
            order_of_images=ImageReferences(name="order_of_images", data=all_images),
        )
        all_unwarped_images_container = Images(
            name=f'{stimulus_template.name}_unwarped',
            description=f'{description} unwarped',
            images=all_images_unwarped,
            order_of_images=ImageReferences(name="order_of_images", data=all_images_unwarped),
        )

        new_stimulus_templates[k] = all_images_container
        new_stimulus_templates[k + "_unwarped"] = all_unwarped_images_container

    # Remove old stimulus templates
    for k in original_stimulus_keys:
        nwbfile.stimulus_template.pop(k)

    # Add new stimulus templates and update IndexSeries references
    for k in original_stimulus_keys:
        # Add unwarped images first so BuildManager can find them when building warped images
        nwbfile.add_stimulus_template(new_stimulus_templates[k + "_unwarped"])
        nwbfile.add_stimulus_template(new_stimulus_templates[k])

        # Validate that matching IndexSeries exists
        if k not in nwbfile.stimulus:
            warnings.warn(
                f"No matching IndexSeries found in stimulus presentations for template '{k}'. "
                f"Available stimulus presentations: {list(nwbfile.stimulus.keys())}"
            )
        else:
            # replace references in stimulus presentation IndexSeries
            index_series = nwbfile.stimulus[k]
            assert isinstance(index_series, IndexSeries), \
                f"Expected stimulus presentation '{k}' to be of type IndexSeries"
            
            if hasattr(index_series, 'indexed_timeseries'):
                index_series.fields['indexed_timeseries'] = None
                index_series.fields['indexed_images'] = new_stimulus_templates[k]
                if index_series.description is None or index_series.description == "no description":
                    index_series.fields['description'] = f"Timestamps and indices of the {k} stimulus template presentations."
            else:
                raise ValueError(
                    f"IndexSeries '{k}' missing 'indexed_timeseries' field"
                )


def combine_probe_file_info(base_nwbfile: NWBFile, probe_nwbfile: NWBFile) -> NWBFile:
    """ Combine LFP and CSD data from a probe NWB file into the main NWB file."""

    # Validate base nwbfile metadata matches probe nwbfile metadata
    assert base_nwbfile.session_start_time == probe_nwbfile.session_start_time, \
        f"Session start times do not match: base='{base_nwbfile.session_start_time}', probe='{probe_nwbfile.session_start_time}'"
    assert base_nwbfile.timestamps_reference_time == probe_nwbfile.timestamps_reference_time, \
        f"Timestamps reference times do not match: base='{base_nwbfile.timestamps_reference_time}', probe='{probe_nwbfile.timestamps_reference_time}'"
    assert set(probe_nwbfile.devices.keys()).issubset(set(base_nwbfile.devices.keys())), \
        f"Probe file references devices not in base file: {set(probe_nwbfile.devices.keys()) - set(base_nwbfile.devices.keys())}"
    assert base_nwbfile.subject.subject_id == probe_nwbfile.subject.subject_id, \
        f"Subject IDs do not match: base='{base_nwbfile.subject.subject_id}', probe='{probe_nwbfile.subject.subject_id}'"

    # Build mapping from probe indices to main file indices based on electrode IDs
    probe_electrode_ids = probe_nwbfile.electrodes.id[:]
    main_electrode_ids = base_nwbfile.electrodes.id[:]

    electrode_mapping = {}
    for old_idx, electrode_id in enumerate(probe_electrode_ids):
        matching_indices = [i for i, main_id in enumerate(main_electrode_ids) if main_id == electrode_id]
        assert len(matching_indices) == 1, \
            f"Expected exactly one matching electrode for ID {electrode_id}, found {len(matching_indices)}"
        electrode_mapping[old_idx] = matching_indices[0]

    acquisition_name = f'probe_{probe_nwbfile.identifier}_lfp'
    lfp_container = probe_nwbfile.acquisition[acquisition_name]
    old_electrical_series = lfp_container[f'{acquisition_name}_data']
    old_electrical_series.reset_parent()

    # Use an iterator to read LFP data in chunks so we don't have to load the
    # entire dataset into memory at once
    data_iterator = H5DatasetDataChunkIterator(
        dataset=old_electrical_series.data,
        chunk_shape=old_electrical_series.data.chunks,
        buffer_gb=8,
    )
    # Rechunk LFP data to optimize for cloud computing and reduce number of chunks.
    # Previously there were about 20K chunks, each about 85 KB large, which is very
    # suboptimal for both read and write.
    # Code Ocean limits the rate of COPY requests per S3 prefix so we cannot have
    # too many chunks per Zarr array or else we get a 503 Slow Down error from S3.
    # This rechunks the LFP data to about 325 chunks, each about 10 MB large after
    # compression.
    old_electrical_series.fields['data'] = ZarrDataIO(
        data=data_iterator,
        chunks=(400_000, 8),
        compressor=GZip(level=9),
    )

    # Create new electrode table region with updated indices
    old_electrodes = old_electrical_series.electrodes
    new_electrode_indices = [electrode_mapping[idx] for idx in old_electrodes.data]
    new_electrodes_region = base_nwbfile.create_electrode_table_region(
        region=new_electrode_indices,
        description=old_electrodes.description,
    )

    # Create new LFP container with the updated electrical series
    # WARNING: this is a workaround to modify an attribute that should not be able to be reset, 
    # validation should always be performed afterwards
    new_lfp = LFP(name=lfp_container.name, electrical_series=old_electrical_series)
    old_electrical_series.electrodes.reset_parent()
    old_electrical_series.fields['electrodes'] = new_electrodes_region
    old_electrical_series.fields['electrodes'].parent = old_electrical_series
    
    # Modify CSD container to have unique name
    # WARNING: this is a workaround to modify a name but is not recommended, 
    # validation should always be performed afterwards
    csd = probe_nwbfile.processing['current_source_density']['ecephys_csd']
    csd.reset_parent()
    csd._AbstractContainer__name = f'probe_{probe_nwbfile.identifier}_ecephys_csd'

    # Add ecephys processing module with lfp data
    if 'ecephys' not in base_nwbfile.processing.keys():
        base_nwbfile.create_processing_module(
            name='ecephys',
            description=(
                "Processed ecephys data from individual probes. Includes LFP and "
                f"{probe_nwbfile.processing['current_source_density'].description}."
            )
        )
    
    base_nwbfile.add_acquisition(new_lfp)
    base_nwbfile.processing['ecephys'].add(csd)
    
    return base_nwbfile


def add_missing_descriptions(nwbfile: NWBFile) -> None:
    """Add missing descriptions to NWB file based on the technical white paper.
    
    Args:
        nwbfile: The NWBFile object to add descriptions to.
    Returns:
        None, the NWBFile object is modified in place.
    """

    # Add units table description
    if hasattr(nwbfile, 'units') and nwbfile.units is not None:
        # Add descriptions for unit metrics columns from technical white papers and documentation
        # See https://allensdk.readthedocs.io/en/latest/_static/examples/nb/visual_behavior_neuropixels_quality_metrics.html
        # See https://brainmapportal-live-4cc80a57cd6e400d854-f7fdcae.divio-media.net/filer_public/80/75/8075a100-ca64-429a-b39a-569121b612b2/neuropixels_visual_coding_-_white_paper_v10.pdf
        unit_column_descriptions = {
            'amplitude': 'Difference (in microvolts) between the peak and trough of the waveform on a single channel.',
            'spread': 'Spatial extent (in microns) of channels where the waveform amplitude exceeds 12% of the peak amplitude.',
            'waveform_duration': 'Difference (in ms) of the time of the waveform peak and trough on the channel with maximum amplitude.',
            'snr': 'Signal-to-noise ratio. Ratio between the waveform amplitude and 2x the standard deviation of the residual waveforms.',            
            'firing_rate': 'Overall firing rate N/T, where N = number of spikes in the complete session and T = total time of the recording session in seconds.',
            'presence_ratio': 'Fraction of 100 equal-sized time blocks that include 1 or more spikes from the unit. Units with low presence ratio likely drifted out of the recording, or could not be tracked by Kilosort2 for the duration of the experiment.',
            'max_drift': 'Maximum range of the median peak channel within 51 s intervals throughout the session. Used to identify sessions with high probe motion.',
            'silhouette_score': 'Standard metric of cluster quality computed by pairwise comparison between the PCs of the cluster and PCs of all other units with overlapping channels. Minimum silhouette score across all pairs (between -1 and 1, with 1 indicating perfect isolation).',
            'isi_violations': 'Relative firing rate of contaminating spikes based on refractory period violations (<1.5 ms). Indicates whether unit contains spikes from multiple neurons.',
            'amplitude_cutoff': 'Approximation of the unit false negative rate based on the spike amplitude distribution. Values closer to 0.5 indicate >50% of spikes may be missing.',
            'isolation_distance': 'Take the center of the cluster in PC space and compute the Mahalanobis distance squared required to find the same number of “other” spikes as the total number of spikes for the unit. The better the cluster quality, the higher the isolation distance.',
            'l_ratio': 'Sum of (1 - chi^2 CDF) for "other" spikes within the isolation distance sphere, divided by total "other" spikes. Lower values indicate better isolation.',
            'd_prime': 'Separability of the unit from all other units based on linear discriminant analysis in PC space.',
            'nn_miss_rate': 'Fraction of spikes from other units that have their nearest neighbors belonging to this unit.',
            'nn_hit_rate': 'Fraction of the four nearest spikes in PC space that belong to this unit.',
            'PT_ratio': 'Ratio of peak amplitude to trough amplitude for the 1D waveform (waveform on peak channel).',
            'recovery_slope': 'Slope of the recovery of 1D waveform (waveform on peak channel) to baseline after repolarization (coming down from peak).',
            'repolarization_slope': 'Maximum slope of the 1D waveform (waveform on peak channel) to baseline after trough.',
            'velocity_below': 'Slope of spike propagation velocity traveling in ventral direction from soma (note to avoid infinite values, this is actually the inverse of velocity: ms/mm).',
            'velocity_above': 'Slope of spike propagation velocity traveling in dorsal direction from soma (note to avoid infinite values, this is actually the inverse of velocity: ms/mm).',
            'quality': 'Label assigned based on waveform shape. Either "good" for physiological waveforms or "noise" for artifactual waveforms.',
            'peak_channel_id': 'Channel ID with the maximum amplitude waveform for this unit.',
        }

        for col_name in nwbfile.units.colnames:
            if (nwbfile.units[col_name].description is None or \
                nwbfile.units[col_name].description == "no description" and \
                col_name in unit_column_descriptions):
                nwbfile.units[col_name].fields['description'] = unit_column_descriptions[col_name]

    # Add descriptions for optogenetic stimulation table
    if 'optotagging' in nwbfile.processing.keys():
        nwbfile.processing['optotagging'].fields['description'] = ("Processing module containing optotagging protocol information.")
        nwbfile.processing['optotagging']['optogenetic_stimulation'].fields['description'] = ("Optogenetic stimulation periods from optotagging protocol during which the "
                                                                   "cortical surface was stimulated with blue light.")

        optostim_column_descriptions = {
            'duration': 'Duration of the optogenetic light stimulus in seconds.',
            'stimulus_name': 'Type of optogenetic stimulus (e.g., pulse, ramp).',
            'level': 'Light level used for this stimulation.',
            'condition': 'Optogenetic stimulus condition.',
        }

        for col_name, description in optostim_column_descriptions.items():
            if col_name in nwbfile.processing['optotagging']['optogenetic_stimulation'].colnames:
                nwbfile.processing['optotagging']['optogenetic_stimulation'][col_name].fields['description'] = description
    
    # Add descriptions for trials table columns
    if hasattr(nwbfile, 'trials') and nwbfile.trials is not None:
        # explanation based on technical white paper and https://allensdk.readthedocs.io/en/latest/_static/examples/nb/aligning_behavioral_data_to_task_events_with_the_stimulus_and_trials_tables.html
        trials_column_descriptions = {
            'initial_image_name': 'Name of the image shown before the change (or sham change) for this trial.',
            'change_image_name': 'Indicates which image was scheduled to be the change image for this trial.',
            'is_change': 'Boolean indicating whether an image change occurred during this trial (True for go and catch trials, False for aborted trials).',
            'change_time': 'Experiment time when the task-control computer commanded an image change.',
            'go': 'Boolean indicating whether trial was a go trial. To qualify as a go trial, an image change must occur and the trial cannot be autorewarded.',
            'catch': 'Boolean indicating whether this trial was a "catch" trial. To qualify as a catch trial, a "sham" change must occur during which the image identity does not change.',
            'response_time': 'Indicates the time when the first lick was registered by the task control software for trials that were not aborted (go or catch).',
            'response_latency': 'Latency in seconds of the first lick after the change time (inf if no lick occurred).',
            'reward_time': 'Indicates when the reward command was triggered for hit trials.',
            'reward_volume': 'Volume of water dispensed as a reward for this trial.',
            'hit': 'Boolean indicating whether trial was a hit.',
            'false_alarm': 'Boolean indicating whether trial was a false alarm.',
            'miss': 'Boolean indicating whether trial was a miss.',
            'correct_reject': 'Boolean indicating whether trial was a correct reject.',
            'aborted': 'Boolean indicating whether trial was aborted.',
            'auto_rewarded': 'Boolean indicating autorewarded trial. During autorewarded trials, the reward is automatically triggered after the change regardless of whether the mouse licked within the response window. ',
            'change_frame': 'Stimulus frame index when the change (on go trials) or sham change (on catch trials) occurred.',
            'trial_length': 'Duration of the trial in seconds.',
        }

        for col_name, description in trials_column_descriptions.items():
            if (col_name in nwbfile.trials.colnames and
                (nwbfile.trials[col_name].description is None or
                 nwbfile.trials[col_name].description == "no description")):
                nwbfile.trials[col_name].fields['description'] = description

    # Add descriptions for stimulus presentations table (stored in intervals)
    if hasattr(nwbfile, 'intervals') and nwbfile.intervals is not None:
        # Check for grating_presentations or other stimulus presentation intervals
        for interval_name in nwbfile.intervals.keys():
            if 'presentations' in interval_name: # TODO - check if there are other cases of this
                stimulus_table = nwbfile.intervals[interval_name]

                # explanation based on technical white paper and https://allensdk.readthedocs.io/en/latest/_static/examples/nb/aligning_behavioral_data_to_task_events_with_the_stimulus_and_trials_tables.html
                # Descriptions based on Allen SDK documentation
                stimulus_column_descriptions = {
                    'active': 'Boolean indicating when the change detection task (with lick spout available) was run.',
                    'is_sham_change': 'Boolean indicating whether this stimulus presentation was a sham change (catch trial).',
                    'is_image_novel': 'Indicates whether this image has been shown to the mouse in previous training sessions. If True, then this image is novel to the mouse.',
                    'image_set': 'Name of the image set (stimulus block) used for this presentation. Examples include natural images, gratings, gabors, or full-field flashes.',
                    'flashes_since_change': 'Number of image flashes of the same image that have occurred since the last stimulus change.',
                    'omitted': 'Boolean indicating whether this image presentation was omitted (replaced with gray screen).',
                    'is_change': 'Boolean indicating whether the image identity changed for this stimulus presentation. When both this and "active" are True, the mouse is rewarded for licking within the response window.',
                    'end_frame': 'Stimulus frame index when this stimulus presentation ended.',
                    'start_frame': 'Stimulus frame index when this stimulus presentation started.',
                    'duration': 'Duration of this stimulus in seconds.',
                    'image_name': 'Name of the image presented during this stimulus flash.',
                }

                for col_name, description in stimulus_column_descriptions.items():
                    if col_name in stimulus_table.colnames:
                        if (stimulus_table[col_name].description is None or
                            stimulus_table[col_name].description.lower() == "no description"):
                            stimulus_table[col_name].fields['description'] = description


def fix_vector_index_dtypes(nwbfile: NWBFile) -> NWBFile:
    """Fix VectorIndex dtypes to use minimal unsigned integer types as per NWB spec.

    VectorIndex objects should use uint8, uint16, uint32, or uint64 depending on the
    maximum value. This function goes through known tables and converts their VectorIndex
    columns to use dtype=uint64.

    Args:
        nwbfile: The NWBFile object to fix

    Returns:
        The modified NWBFile object with corrected VectorIndex dtypes
    """
    def fix_table_indices(table):
        """Fix VectorIndex columns in a DynamicTable."""
        if table is None:
            return

        for col_name in table.colnames:
            col = table[col_name]
            if isinstance(col, VectorIndex):
                data = col.data
                if data is not None and len(data) > 0:
                    target_dtype = np.dtype('uint64')
                    if data.dtype != target_dtype:
                        # WARNING: This is a workaround to modify a protected attribute,
                        # validation should always be performed afterwards
                        col._Data__data = data[:].astype(target_dtype)

    # Fix units table
    if hasattr(nwbfile, 'units') and nwbfile.units is not None:
        fix_table_indices(nwbfile.units)

    # Fix intervals tables
    if hasattr(nwbfile, 'intervals') and nwbfile.intervals is not None:
        for interval_table in nwbfile.intervals.values():
            fix_table_indices(interval_table)

    # Fix processing modules tables (e.g., optotagging)
    if hasattr(nwbfile, 'processing'):
        for module in nwbfile.processing.values():
            for data_interface in module.data_interfaces.values():
                # Check if it's a TimeIntervals table or similar
                if hasattr(data_interface, 'colnames'):
                    fix_table_indices(data_interface)

    return nwbfile


def inspect_zarr_file(zarr_path: Path, inspector_report_path: Path) -> None:
    """Inspect a Zarr NWB file using nwbinspector and save the report to a text file.
    """
    # PyNWB validation does not yet support Zarr paths, but we can use NWBZarrIO to get an IO object
    # and validate that.
    messages = list(_inspect_zarr_file_helper(zarr_path=zarr_path))
    
    # Format and print messages to text file
    if messages:
        formatted_messages = format_messages(messages=messages, levels=["file_path", "importance"])
        save_report(
            report_file_path=inspector_report_path,
            formatted_messages=formatted_messages,
            overwrite=True,
        )


def _inspect_zarr_file_helper(zarr_path: Path) -> Iterable[Optional[InspectorMessage]]:
    """Helper function to inspect a Zarr NWB file and yield InspectorMessages."""
    config = load_config("dandi")
    io = None
    try:
        io = NWBZarrIO(zarr_path, mode='r')
        in_memory_nwbfile = io.read()

        validation_result = validate(io=io)
        if isinstance(validation_result, tuple):
            validation_errors = validation_result[0]
        else:
            validation_errors = validation_result

        for validation_error in validation_errors:
            yield InspectorMessage(
                message=validation_error.reason,
                importance=Importance.PYNWB_VALIDATION,
                check_function_name=validation_error.name,
                location=validation_error.location,
                file_path=zarr_path,
            )

        for inspector_message in inspect_nwbfile_object(
            nwbfile_object=in_memory_nwbfile,
            config=config,
        ):
            inspector_message.file_path = zarr_path
            yield inspector_message

    except Exception as exception:
        exception_name = f"{type(exception).__module__}.{type(exception).__name__}"
        yield InspectorMessage(
            message=traceback.format_exc(),
            importance=Importance.ERROR,
            check_function_name=(
                f"During io.read(), an error occurred: {exception_name}. "
                f"This indicates that PyNWB was unable to read the file. "
                f"See the traceback message for more details."
            ),
            file_path=zarr_path,
        )
    finally:
        if io is not None:
            io.close()  # close the io object in case of exceptions or when inspection is complete
