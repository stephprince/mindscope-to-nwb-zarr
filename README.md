# mindscope-to-nwb-zarr

This repository is set up as a Code Ocean capsule to convert Mindscope NWB files from the HDF5 format to Zarr format, extract AIND Metadata JSON files, and document changes made during the conversion process.

## Running on Code Ocean

The Code Ocean capsule is set up with an App Builder with a single input parameter:
- `dataset`: Options are "Visual Behavior 2p" and "Visual Behavior Neuropixels". This parameter selects which dataset to convert.

To run the conversion on Code Ocean, sync the capsule, enter the desired dataset option, and click "Run with parameters".

## Testing Locally

1. `cd code`
2. `uv run python run_capsule.py --dataset "Visual Behavior 2p"` (or "Visual Behavior Neuropixels")
  - Note: This will create a virtual environment in the `code/.venv` directory and a `uv.lock` file if these do not already exist.

Note: Local testing requires access to the Mindscope NWB HDF5 files, which are not included in this repository. These files can be downloaded from the Allen Institute's data portal and must be placed in the appropriate directory structure under `data/`.

Note: By default, Windows has a maximum path length of 260 characters, which may cause issues when working with Zarr stores due to their nested directory structure. If you encounter path length issues, consider enabling long paths in Windows or changing the results folder to a location with a shorter path.

## Recommended Future Improvements for Conversion from HDF5 to Zarr

- Optimize Zarr array chunking shapes to improve read/write performance.
- Add missing descriptions for table columns and other NWB objects.
- In `TimeSeries` objects that have timestamp arrays with regular sampling rates, use `starting_time` and `rate` attributes instead of storing full timestamp arrays to reduce file size.
- Make how stimulus presentation times and parameters are stored consistent across datasets.

Visual Behavior Ephys
- Move optotagging intervals table to top-level `nwbfile.intervals`
- Set an appropriate "strain" value for the `Subject` object instead of using the placeholder "unknown".
- Rename/reorganize processing modules to be more conventional:
   * stimulus -> behavior (?)
   * running -> behavior
   * rewards -> behavior
   * optotagging -> ogen
   * licking -> behavior
   * eye_tracking_rig_metadata -> behavior
   * current_source_density -> ecephys
- Add descriptions to several objects in the file
   * Add descriptions to is_sham_change, active, trials_id, flashes_since_change, end_frame, start_frame, duration, position_y, position_x, color, rewarded, omitted, is_image_novel, is_change, image_name columns
   * Add descriptions to stimulus/timestamps, /processing/running/speed_unfiltered, /processing/runnings/speed, /processing/rewards/volume, /processing/rewards/autorewarded, /processing/current_source_density/ecephys_csd/current_source_density, /acquisition/probe_1158270876_lfp/probe_1158270876_lfp_data, timeseries

Visual Coding Ephys
- Set an appropriate "strain" value for the `Subject` object instead of using the placeholder "unknown".
- Add stimulus template data for natural scenes, natural movies, and natural movie shuffled stimuli
- Add descriptions to several objects in the file (see inspector report for full information)
