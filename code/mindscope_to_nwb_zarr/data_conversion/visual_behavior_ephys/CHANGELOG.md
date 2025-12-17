## Visual Behavior - Neuropixels

### Implemented

### TODO 

* Combine probe file LFP + CSD data into single NWB file # TODO - decide about where to put LFP data
    * add note about how different LFP objects may come from different probes
* Move LFP data into processing / LFP module (if it underwent processing? Otherwise leave in acquisition)
* Change the stimulus_template ImageSeries to a set of Image objects in an Images container, and change the IndexSeries to link to this Images container (will solve CriticalError and nan values)
* move optotagging intervals table to top level nwbfile.intervals
* remove unused 'imp' column from electrodes table
* add missing experiment description
* rename processing modules 
   * stimulus -> behavior (?)
   * running -> behavior
   * rewards -> behavior
   * optotagging -> ogen
   * licking -> behavior
   * eye_tracking_rig_metadata -> behavior
   * current_source_density -> ecephys
* add description to several objects in the file
   * units table
   * waveform_duration, velocity_below, velocity_above, spread, snr, silhouette_score, repolarization_slope, recovery_slope, quality, presence_ratio, peak_channel_id, nn_miss_rate, nn_hit_rate, max_drift, local_index, l_ratio, isolation_distance, isi_violations, firing_rate, d_prime, cumulative_drift, cluster_id, amplitude_cutoff, amplitude, PT_ratio columns in units table
   * add description to is_sham_change, active, trials_id, flashes_since_change, end_frame, start_frame, duration, position_y, position_x, color, rewarded, omitted, is_image_novel, is_change, image_name columns
   * add descriptions to stimulus/timestamps, /processing/running/speed_unfiltered, /processing/runnings/speed, /processing/rewards/volume, /processing/rewards/autorewarded, /processing/current_source_density/ecephys_csd/current_source_density, /acquisition/probe_1158270876_lfp/probe_1158270876_lfp_data, timeseries
   * add description to optogenetic_stimulation time intervals
   