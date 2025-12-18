import warnings

from pathlib import Path
from pynwb import NWBFile, validate
from pynwb.ecephys import LFP
from pynwb.image import Images, GrayscaleImage
from hdmf_zarr.nwb import NWBZarrIO
from nwbinspector import inspect_nwbfile_object, format_messages, save_report


def convert_stimulus_template_to_images(nwbfile: NWBFile) -> NWBFile:
    """Convert stimulus_template from ImageSeries to Images container with IndexSeries references."""

    # Find the stimulus template (typically in stimulus.templates)
    if len(nwbfile.stimulus_template) < 1:
        warnings.warn("NWBFile has no stimulus_template field populated. Skipping conversion.")
        return nwbfile

    original_stimulus_keys = list(nwbfile.stimulus_template.keys())
    new_stimulus_templates = []
    for k in original_stimulus_keys:
        stimulus_template = nwbfile.stimulus_template[k]
        image_data = stimulus_template.data[:]  # Shape should be (num_images, height, width)
        image_data_unwarped = stimulus_template.unwarped[:]

        # adapt description
        description = 'Natural scene images' if 'Natural_Images' in stimulus_template.name else stimulus_template.description

        # create images objects
        all_images = []
        all_images_unwarped = []
        for i in range(image_data.shape[0]):
            all_images.append(GrayscaleImage(
                                name=stimulus_template.control_description[i],
                                data=image_data[i],
                                description=f"Natural scene image {stimulus_template.control[i]}",
                            ))
            all_images_unwarped.append(GrayscaleImage(
                                name=stimulus_template.control_description[i],
                                data=image_data_unwarped[i],
                                description=f"Natural scene image unwarped {stimulus_template.control[i]}",
                            ))

        new_stimulus_templates.append(Images(name=stimulus_template.name,
                                  description=description,
                                  images=all_images))
        new_stimulus_templates.append(Images(name=f'{stimulus_template.name}_unwarped',
                                           description=f'{description} unwarped',
                                           images=all_images_unwarped))

    # remove old stimulus templates and add new ones
    for k in original_stimulus_keys:
        nwbfile.stimulus_template.pop(k)

    for new_template in new_stimulus_templates:
        nwbfile.add_stimulus_template(new_template)

    return nwbfile

def combine_probe_file_info(base_nwbfile: NWBFile, probe_nwbfile: NWBFile) -> NWBFile:
    """ Combine LFP and CSD data from a probe NWB file into the main NWB file."""

    # Build mapping from probe indices to main file indices based on electrode IDs
    probe_electrode_ids = probe_nwbfile.electrodes.id[:]
    main_electrode_ids = base_nwbfile.electrodes.id[:]

    electrode_mapping = {}
    for old_idx, electrode_id in enumerate(probe_electrode_ids):
        matching_indices = [i for i, main_id in enumerate(main_electrode_ids) if main_id == electrode_id]
        assert len(matching_indices) == 1, f"Expected exactly one matching electrode for ID {electrode_id}, found {len(matching_indices)}"
        electrode_mapping[old_idx] = matching_indices[0]

    acquisition_name = f'probe_{probe_nwbfile.identifier}_lfp'
    lfp_container = probe_nwbfile.acquisition[acquisition_name]
    old_electrical_series = lfp_container[f'{acquisition_name}_data']
    old_electrical_series.reset_parent()

    # Create new electrode table region with updated indices
    old_electrodes = old_electrical_series.electrodes
    new_electrode_indices = [electrode_mapping[idx] for idx in old_electrodes.data]
    new_electrodes_region = base_nwbfile.create_electrode_table_region(
        region=new_electrode_indices,
        description=old_electrodes.description,
    )

    # Create new LFP container with the updated electrical series
    # WARNING: this is a workaround to modify an attribute that should not be able to be reset, validation should always be performed afterwards
    new_lfp = LFP(name=lfp_container.name, electrical_series=old_electrical_series)
    new_lfp[f'{acquisition_name}_data']._remove_child(new_lfp[f'{acquisition_name}_data'].electrodes)
    new_lfp[f'{acquisition_name}_data'].fields['electrodes'] = new_electrodes_region
    new_lfp[f'{acquisition_name}_data'].fields['electrodes'].parent = new_lfp[f'{acquisition_name}_data']
    
    # Modify CSD container to have unique name
    # WARNING: this is a workaround to modify a name but is not recommended, validation should always be performed afterwards
    csd = probe_nwbfile.processing['current_source_density']['ecephys_csd']
    csd.reset_parent()
    csd._AbstractContainer__name = f'probe_{probe_nwbfile.identifier}_ecephys_csd'

    # Add ecephys processing module with lfp data
    if 'ecephys' not in base_nwbfile.processing.keys():
        base_nwbfile.create_processing_module(name='ecephys',
                                            description=("Processed ecephys data from individual probes. Includes LFP and "
                                                        f"{probe_nwbfile.processing['current_source_density'].description}."))
    
    base_nwbfile.processing['ecephys'].add(new_lfp)
    base_nwbfile.processing['ecephys'].add(csd)
    
    return base_nwbfile

def add_missing_descriptions(nwbfile: NWBFile) -> NWBFile:
    """Add missing descriptions to NWB file based on the technical white paper."""

    if nwbfile.experiment_description is None:
        nwbfile.experiment_description = ("The Visual Behavior Neuropixels project utilized the "
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
                                        "each mouse for up to two consecutive recording days.")

    # Add units table description
    if hasattr(nwbfile, 'units') and nwbfile.units is not None:
        nwbfile.units.fields['description'] = ("Units identified from spike sorting using Kilosort2. "
                                     "Note that unlike the data from the Visual Coding Neuropixels pipeline, "
                                     "for which potential noise units were filtered from the released "
                                     "dataset, we have elected to return all units for the Visual Behavior "
                                     "Neuropixels dataset.")

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

    # TODO - Add descriptions for stimulus presentations table columns
    # TODO - Add descriptions for processing modules             

    return nwbfile

def inspect_zarr_file(zarr_filename):
    with NWBZarrIO(zarr_filename, mode='r') as zarr_io:
        nwbfile = zarr_io.read()

        # inspect nwb file with io object
        # NOTE - this does not run pynwb validation, will run that separately
        messages = list(inspect_nwbfile_object(nwbfile))

        # format and print messages nicely
        if messages:
            formatted_messages = format_messages(
                messages=messages,
                levels=["importance", "file_path"],
                reverse=[True, False]
            )
            save_report(report_file_path=f"data/{Path(zarr_filename).stem}_report.txt", 
                        formatted_messages=formatted_messages,
                        overwrite=True)

        # validate file with IO object
        # TODO - waiting to fix hdmf-zarr related validation issues before including
        validation_errors = validate(io=zarr_io)
        print(validation_errors)