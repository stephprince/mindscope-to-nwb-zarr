## Visual Behavior - Neuropixels

### Source NWB HDF5 files

The source HDF5-based NWB files for Visual Behavior - Neuropixels data can be found in the S3 bucket `s3://visual-behavior-neuropixels-data` under the paths `visual-behavior-neuropixels/behavior_ecephys_sessions` (behavior+ephys sessions) and `visual-behavior-neuropixels/behavior_only_sessions` (behavior-only sessions). The session metadata in `visual-behavior-neuropixels/project_metadata/behavior_sessions.csv` and `visual-behavior-neuropixels/project_metadata/ecephys_sessions.csv` was used to obtain session information for the conversion.

There are 3424 sessions in total based on behavior_sessions.csv. Sessions can be behavior-only or behavior+ephys. Behavior+ephys sessions have one base NWB file (`ecephys_session_{ecephys_session_id}.nwb`) plus multiple probe files (`probe_{probe_id}.nwb`) containing LFP and CSD data. Behavior-only sessions have one NWB file (`behavior_session_{behavior_session_id}.nwb`).

### Changes made when migrating from HDF5 to Zarr

- Updated to use latest NWB schema 2.9.0 and HDMF Common schema 1.8.0.
- Combined probe files containing LFP + CSD data into single NWB file.
- Converted `StimulusTemplate` stimulus templates which had `NaN` timestamps to be stored as `WarpedStimulusTemplateImage` objects in an `Images` container to follow best practices for storing stimulus template data.
  - Added unwarped images as `GrayscaleImage` objects in a separate `Images` container.
  - Adjusted the corresponding stimulus presentation `IndexSeries` to reference the `Images` container containing `WarpedStimulusTemplateImage` objects.
- Added missing experiment description if needed.
- Added missing description fields to several objects in the file based on information from the Visual Behavior technical white paper.
