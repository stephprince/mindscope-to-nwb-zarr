## Visual Coding - Neuropixels

### Source HDF5 Files



### Changes made when migrating from HDF5 to Zarr
- Updated to use NWB Schema version 2.9.0 from version 2.2.2 and HDMF Common Schema version 1.8.0 from version 1.1.3.
- Combined probe files containing LFP + CSD data into single NWB file
- Moved LFP data into processing module (best practice for storing downsampled and lowpass filtered data)
- Added missing experiment description if needed
- Added description to several objects in the file
   - Units table and related columns
   - Trials table and related columns
   - Stimulus presentations table and related columns
   - Optogenetic_stimulation time intervals 
- Converted VectorIndex dtypes to be unsigned integers to be compliant with the latest NWB Schema
