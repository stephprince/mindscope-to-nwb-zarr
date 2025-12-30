## Visual Coding - Neuropixels

### Source NWB HDF5 files

The source HDF5-based NWB files for Visual Coding - Neuropixels data can be found in the S3 bucket `s3://allen-brain-observatory` under the path `visual-coding-neuropixels/ecephys-cache/`. The session metadata in `visual-coding-neuropixels/ecephys-cache/sessions.csv` was used to obtain session information for the conversion.

There are 58 sessions in total. Each session has one base NWB file (`session_{session_id}.nwb`) containing units, electrodes, and other session data, plus multiple probe LFP files (`probe_{probe_id}_lfp.nwb`) containing LFP and CSD data for each probe.

### Changes made when migrating from HDF5 to Zarr
- Updated to use NWB Schema version 2.9.0 from version 2.2.2 and HDMF Common Schema version 1.8.0 from version 1.1.3.
- Combined probe files containing LFP + CSD data into single NWB file
- Added missing experiment description if needed
- Added description to several objects in the file
   - Units table and related columns
   - Trials table and related columns
   - Stimulus presentations table and related columns
   - Optogenetic_stimulation time intervals 
- Converted VectorIndex dtypes to be unsigned integers to be compliant with the latest NWB Schema
- Rechunked LFP data to have chunk shape `(500000, 8)` to improve read and write performance when using Zarr storage and for more reliable write performance during conversion in a Code Ocean pipeline where publishing the Zarr store to S3 is limited in the number of COPY requests per second for an S3 prefix.

