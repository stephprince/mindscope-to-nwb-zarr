## Visual Behavior - Neuropixels

### Implemented
* Combined probe files containing LFP + CSD data into single NWB file
* Moved LFP data into processing module (best practice for storing downsampled and lowpass filtered data)
* Change the StimulusTemplate ImageSeries type in stimulus_template to a WarpedStimulusTemplateImage type that is a set of Image objects in an Images container
   * added unwarped images as a separate Images container
   * updated IndexSeries references to the StimulusTemplate ImageSeries type to reference the new WarpedStimulusTemplateImage object
* add missing experiment description if needed
* add description to several objects in the file
   * units table
   * add description to optogenetic_stimulation time intervals
   * waveform_duration, velocity_below, velocity_above, spread, snr, silhouette_score, repolarization_slope, recovery_slope, quality, presence_ratio, peak_channel_id, nn_miss_rate, nn_hit_rate, max_drift, local_index, l_ratio, isolation_distance, isi_violations, firing_rate, d_prime, cumulative_drift, cluster_id, amplitude_cutoff, amplitude, PT_ratio columns in units table

### TODO 
* move optotagging intervals table to top level nwbfile.intervals
* rename processing modules 
   * stimulus -> behavior (?)
   * running -> behavior
   * rewards -> behavior
   * optotagging -> ogen
   * licking -> behavior
   * eye_tracking_rig_metadata -> behavior
   * current_source_density -> ecephys
* add description to several objects in the file
   * add description to is_sham_change, active, trials_id, flashes_since_change, end_frame, start_frame, duration, position_y, position_x, color, rewarded, omitted, is_image_novel, is_change, image_name columns
   * add descriptions to stimulus/timestamps, /processing/running/speed_unfiltered, /processing/runnings/speed, /processing/rewards/volume, /processing/rewards/autorewarded, /processing/current_source_density/ecephys_csd/current_source_density, /acquisition/probe_1158270876_lfp/probe_1158270876_lfp_data, timeseries
