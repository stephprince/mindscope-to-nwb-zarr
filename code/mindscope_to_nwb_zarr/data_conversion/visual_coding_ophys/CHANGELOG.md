## Visual Coding - 2p

### Source HDF5 files

HDF5-based NWB 2.0 files were sourced from the DANDI Archive dandiset 000728, version 0.240827.1809 (https://dandiarchive.org/dandiset/000728/0.240827.1809).

There are 1518 sessions in total. For each session, there are 2 HDF5 NWB files. One NWB file contains session metadata and processed 2p data. The second NWB file contains only the raw 2p imaging data.

These files were converted from NWB 1.0 files using this [conversion pipeline](https://github.com/catalystneuro/visual-coding-to-nwb-v2) in August 2024.

### Changes made when migrating from HDF5 to Zarr
- Updated to use NWB Schema version 2.9.0 from version 2.7.0.
- Updated subject ID to be the value of the "donor_name" field for the experiment/session from the AllenSDK Brain Observatory API. This comes from `specimen/donor/external_donor_name` from `allen-brain-observatory/visual-coding-2p/ophys_experiments.json` on S3. It is a 6 digit number. These files are named according to the DANDI schema, e.g., `sub-<donor_name>_ses-<experiment_id>_behavior+image+ophys.zarr`.
  - Originally, the NWB 1.0 files were named with the experiment ID, e.g., `allen-brain-observatory/visual-coding-2p/ophys_experiment_data/<experiment_id>.nwb`.
  - The HDF5-based NWB 2.0 files had subject ID = specimen ID (`specimen_id` in `ophys_experiments.json`), and session ID = "<experiment ID>-<stimulus set ID>" (where `experiment ID`=`id` in `ophys_experiments.json` and `stimulus set ID` is from {"StimA", "StimB", "StimC"} depending on the value of `stimulus_name` in `ophys_experiments.json`). These files were named according to the DANDI schema, e.g., `sub-<specimen_id>_ses-<experiment_id>_behavior+image+ophys.nwb` (metadata and processed 2p data) and `sub-<specimen_id>_ses-<experiment_id>_ophys.nwb` (raw 2p data).
- TODO: Added new extension `ndx-aibs-visual-coding-2p` version 0.1.0 that specifies a new neurodata type `OphysExperimentMetadata` that extends `LabMetaData` and stores a JSON string containing information from AllenSDK Brain Observatory Visual Coding 2p experiment metadata that was not included in the original NWB 1.0 files or the updated NWB 2.0 files. Used this extension to add a new `OphysExperimentMetadata` object to the NWB file with this information. 
- Converted `ImageSeries` stimulus templates which had `NaN` rate and starting time to be stored as `Image` objects in an `Images` container. Adjusted the corresponding stimulus presentation `IndexSeries` to reference the `Images` container.