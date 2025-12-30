## Visual Behavior - 2p

### Source NWB HDF5 files

The source HDF5-based NWB files for Visual Behavior - 2p data can be found in the S3 bucket `s3://visual-behavior-ophys-data` under the paths `visual-behavior-ophys/behavior_ophys_experiments` and `visual-behavior-ophys/behavior_sessions`. The experiment metadata in `visual-behavior-ophys/project_metadata/behavior_session_table.csv` was used to obtain additional metadata for the conversion.

### Changes made when migrating from HDF5 to Zarr

- Updated to use latest NWB schema version 2.9.0 from version 2.6.0-alpha.
- Used new versions of ndx-aibs-stimulus-template (0.2.0) and ndx-ellipse-eye-tracking (0.2.0) NWB extensions, defined in this repository, to follow best practices for storing stimulus template data and custom eye tracking data.
- For multiscope sessions, each experiment was originally stored in separate NWB HDF5 files, each containing one `ImagingPlane`, one ophys `ProcessingModule`, and one `OphysBehaviorMetadata` object that were unique to each experiment. There were up to 8 experiments (imaging planes) per session. All `ImagingPlanes`, ophys `ProcessingModule`, and `OphysBehaviorMetadata` objects for a session have been renamed with the suffix "_plane_X", combined into a single NWB file, and then exported to Zarr. 
    - Note: The value of X in the suffix "_plane_X" is 1-indexed and corresponds to the order of the experiment files listed in the `behavior_session_table.csv` metadata table for that session.
    - Note: Some data objects were duplicated across the multiple experiment files for a session (e.g., stimulus table, trials, licking). In the combined NWB file, these objects are only stored once and retain the NWB object ID from the first experiment file listed in the `behavior_session_table.csv` metadata table for that session.
- Set the `NWBFile.session_id` field to be the same as the `NWBFile.identifier` field so that the session ID used for DANDI naming is more similar to the original NWB HDF5 file name on S3.
- Converted `StimulusTemplate` stimulus templates which had `NaN` timestamps to be stored as `WarpedStimulusTemplateImage` objects in an `Images` container to follow best practices for storing stimulus template data. 
  - Added unwarped images as `GrayscaleImage` objects in a separate `Images` container.
  - Adjusted the corresponding stimulus presentation `IndexSeries` to reference the `Images` container containing `WarpedStimulusTemplateImage` objects.
