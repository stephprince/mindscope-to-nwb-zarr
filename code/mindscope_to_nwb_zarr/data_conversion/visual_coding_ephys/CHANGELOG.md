## Visual Coding - Neuropixels

### Implemented
- Updated to use latest NWB schema 2.9.0 and HDMF Common schema 1.8.0.
* Combined probe files containing LFP + CSD data into single NWB file
* Moved LFP data into processing module (best practice for storing downsampled and lowpass filtered data)
* Added missing experiment description if needed
* Added description to several objects in the file
   * units table and related columns
   * trials table and related columns
   * stimulus presentations table and related columns
   * optogenetic_stimulation time intervals 
* Converted VectorIndex dtypes to use minimal unsigned integer type to be compliant with later schema versions
