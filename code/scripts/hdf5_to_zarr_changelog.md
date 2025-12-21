# Changes made when migrating Allen Brain Observatory NWB files from HDF5 to Zarr format

## Visual Behavior 2p

- For multiscope sessions, each experiment was originally stored in separate NWB HDF5 files, each containing one ImagingPlane, one ophys ProcessingModule, and one OphysBehaviorMetadata object that were unique to each experiment. There were up to 8 experiments (imaging planes) per session. All ImagingPlanes, ophys ProcessingModule, and OphysBehaviorMetadata objects for a session have been renamed with the suffix "_plane_X", combined into a single NWB file, and then exported to Zarr. 
    - Note: The value of X in the suffix "_plane_X" is 1-indexed and corresponds to the order of the experiment files listed in the `behavior_session_table.csv` metadata table for that session.
    - Note: Some data objects were duplicated across the multiple experiment files for a session (e.g., stimulus table, trials, licking). In the combined NWB file, these objects are only stored once and retain the NWB object ID from the first experiment file listed in the `behavior_session_table.csv` metadata table for that session.  