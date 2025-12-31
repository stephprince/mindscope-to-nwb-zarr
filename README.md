# mindscope-to-nwb-zarr

This repository is set up as a Code Ocean capsule to convert Mindscope NWB files from the HDF5 format to Zarr format, extract AIND Metadata JSON files, and document changes made during the conversion process.

## Supported Datasets

| Dataset | Data Source | Conversion Time |
|---------|-------------|-----------------|
| Visual Behavior Ephys | S3: `visual-behavior-neuropixels-data` | ~11 min/session |
| Visual Behavior Ophys | S3: `visual-behavior-ophys-data` | ~15-30 sec/session |
| Visual Coding Ephys | S3: `allen-brain-observatory` | ~35 min/session |
| Visual Coding Ophys | DANDI (dandiset 000728) | ~30-48 min/session |

## Running Locally

```bash
cd code
uv run python run_capsule.py --dataset "<dataset_name>" --results_dir "<results_folder>" --metadata False
```

**Parameters:**
- `--dataset`: One of: `"Visual Behavior Ephys"`, `"Visual Behavior Ophys"`, `"Visual Coding Ephys"`, `"Visual Coding Ophys"` (case-insensitive)
- `--results_dir`: Path to output folder for converted Zarr files and metadata (default: `../results/`)
- `--metadata`: Set to `True` to generate only AIND metadata JSON files (no Zarr conversion)

**Example:**
```bash
cd code
uv run python run_capsule.py --dataset "Visual Coding Ophys" --results_dir "./results"
```

**Notes:**
- This command will create a virtual environment in `code/.venv` and a `uv.lock` file if they don't exist.
- Windows has a 260-character path limit which may cause issues with Zarr's nested directory structure. Enable long paths in Windows or use a shorter results path.
- Most datasets require S3 access to the source data. Visual Coding Ophys streams directly from DANDI.

## Running on Code Ocean

### As a Capsule

The Code Ocean capsule uses an App Builder with these parameters:
- `dataset`: Selects which dataset to convert
- `metadata`: Set to `True` for metadata-only generation

Sync the capsule with the GitHub repository, attach the appropriate data assets, configure parameters in the App Builder tab, and click "Run with parameters".

### AIND Metadata Extraction in a Capsule

To extract only AIND metadata JSON files without Zarr conversion, set the `metadata` parameter to `True` in the App Builder tab before running the capsule. This will use the mounted data assets as input and output metadata files to the results folder, and the run does not require paralellization in a capsule, though some datasets take longer to extract. Note that this will skip the Zarr conversion step.

### Batch Conversion using Pipelines

Each dataset has a `create_inputs.py` module that generates numbered input files for pipeline parallelization.

1. **Generate input files locally:**
   ```bash
   cd code
   uv run python -m mindscope_to_nwb_zarr.data_conversion.<dataset_module>.create_inputs
   ```
   Where `<dataset_module>` is one of: `visual_behavior_ephys`, `visual_behavior_ophys`, `visual_coding_ephys`, `visual_coding_ophys`

2. **Create a Code Ocean data asset** with the generated input files

3. **Create a pipeline:**
   - Add the capsule
   - Map paths from the data asset to the capsule
   - Connect to a results bucket
   - Set parameter: `--dataset "<dataset_name>"`

4. **Run the pipeline**


## Project Structure

```
mindscope-to-nwb-zarr/
├── code/
│   ├── run_capsule.py                    # Main entry point
│   ├── mindscope_to_nwb_zarr/
│   │   ├── data_conversion/              # HDF5 to Zarr conversion
│   │   │   ├── conversion_utils.py       # Shared utilities
│   │   │   ├── create_input_utils.py     # Pipeline input file generation
│   │   │   ├── visual_behavior_ephys/
│   │   │   │   ├── run_conversion.py     # Main conversion function
│   │   │   │   └── create_inputs.py      # Pipeline input generation
│   │   │   ├── visual_behavior_ophys/
│   │   │   ├── visual_coding_ephys/
│   │   │   └── visual_coding_ophys/
│   │   ├── aind_data_schema/             # AIND metadata JSON extraction
│   │   │   ├── utils.py                  # Shared metadata utilities
│   │   │   ├── stimuli.py                # Stimulus metadata helpers
│   │   │   ├── visual_behavior_ephys/
│   │   │   │   ├── metadata_generation.py  # Main entry point
│   │   │   │   ├── acquisition.py        # Acquisition metadata
│   │   │   │   ├── data_description.py   # Data description metadata
│   │   │   │   ├── procedures.py         # Procedures metadata
│   │   │   │   └── subject.py            # Subject metadata
│   │   │   ├── visual_behavior_ophys/
│   │   │   ├── visual_coding_ephys/
│   │   │   └── visual_coding_ophys/
│   │   └── pynwb_utils.py                # NWB utilities
│   └── scripts/                          # Utility scripts
├── data/                                 # Input files (git-ignored)
├── notebooks/                            # Usage examples
└── environment/                          # Code Ocean environment
```


## Utility Scripts

| Script | Purpose |
|--------|---------|
| `scripts/compare_hdf5_zarr.py` | Validate conversion by comparing HDF5 vs Zarr contents |
| `scripts/get_mouse_ids_from_allensdk.py` | Download mouse ID metadata from AllensSDK |
| `scripts/metadata_from_allensdk.py` | Extract metadata directly from AllensSDK |
| `scripts/nwb_cached_specs_to_json.py` | Export NWB specification metadata to JSON |


## Conversion Process

Each conversion:
1. Reads source HDF5 NWB file(s) with `NWBHDF5IO`
2. Applies dataset-specific transformations:
   - Converts deprecated `StimulusTemplate` to `Images` containers
   - Adds missing descriptions from technical white papers
   - Combines multi-probe/multi-plane files where applicable
3. Writes to Zarr format with `NWBZarrIO`
4. Validates output with nwbinspector (generates `.inspector_report.txt`)

### Key Transformations

**All Datasets:**
- **Schema and Extension Updates**: Updated NWB schema to 2.9.0 and made minor improvements to internal NWB extensions to address compatibility issues
- **Stimulus Templates**: Converted from deprecated `StimulusTemplate`/`ImageSeries` to modern `Images` containers with `GrayscaleImage` and `WarpedStimulusTemplateImage` objects, and updated `IndexSeries` references to use `indexed_images` instead of `indexed_timeseries`
- **Missing Descriptions**: Added descriptions for unit metrics, trials table columns, stimulus presentation columns, and optogenetic stimulation tables based on technical white papers
- **VectorIndex Dtypes**: Fixed VectorIndex columns to use `uint64` dtype per NWB spec

**Visual Coding Ophys:**
- Combined processed NWB file (metadata + processed 2p data) with raw NWB file (raw 2p imaging) into single Zarr output
- Changed subject ID to external donor name from metadata
- Converted natural movie `ImageSeries` templates to `Images` containers with `GrayscaleImage` frames
- Added `order_of_images` to existing `Images` containers (natural scenes, locally sparse noise)
- Rechunked raw 2p imaging data to (75, 512, width) to reduce chunk count for cloud storage limits

**Visual Behavior Ophys:**
- Combined multiplane sessions (multiple single-plane NWB files) into single Zarr output with renamed imaging planes (`imaging_plane_1`, `imaging_plane_2`, etc.) and processing modules (`ophys_plane_1`, `ophys_plane_2`, etc.)

**Visual Coding Ephys:**
- Combined base session NWB file with multiple probe LFP files into single Zarr output
- Rechunked LFP data to (500,000, 8) with gzip level 9 compression (~10 MB chunks) to reduce chunk count
- Added CSD data from probe files with unique names (`probe_{id}_ecephys_csd`)

**Visual Behavior Ephys:**
- Combined base session NWB file with multiple probe files into single Zarr output
- Rechunked LFP data to (500,000, 8) with gzip level 9 compression
- Added units table description noting all units are returned (unlike Visual Coding which filtered noise units)

See the changelog files in each dataset's `data_conversion` folder for full details.


## Recommended Future Improvements for Conversion from HDF5 to Zarr

### All Datasets
- Optimize Zarr array chunking shapes to improve read/write performance.
- Add missing descriptions for table columns and other NWB objects (see inspector reports for full information).
- In `TimeSeries` objects that have timestamp arrays with regular sampling rates, use `starting_time` and `rate` attributes instead of storing full timestamp arrays to reduce file size.
- Make how stimulus presentation times and parameters are stored consistent across datasets.
- Add `experimenter` to the NWB file.
- Add `keywords` to the NWB file.
- Rename/reorganize processing modules to be more conventional:
   - licking -> behavior
   - rewards -> behavior
   - running -> behavior
   - stimulus -> behavior (?)
- Consider reorganizing eye tracking rig metadata to be under the `general` group or a subtype of `Device` instead of under a processing module.
- Work with the NWB team to evaluate the efficiency and usability of storing many image objects in the Images container (e.g., natural movies, locally sparse noise) in Zarr vs storing them as a stacked array. This is particularly slow on write.
- Add explicit link from stimulus presentation and trials tables to the stimulus template images in the new `Images` container instead of relying on name/indices matching.

### Visual Behavior Ophys
- For multiscope sessions, name the imaging planes based on the order of the imaging planes before QC filtering so that it better matches the AIND metadata which accounts for all imaging planes.
- Remove the cached ndx-aibs-ecephys NWB extension which appears to be unused in these files.

### Visual Behavior Ephys
- Move optotagging intervals table to top-level `nwbfile.intervals`
- Rename/reorganize processing modules to be more conventional:
   - optotagging -> ogen
   - current_source_density -> ecephys
- Convert and add raw highpass data to the NWB file from the S3 bucket.

### Visual Coding Ephys
- Add stimulus template data for natural scenes, natural movies, and natural movie shuffled stimuli
- Convert and add raw highpass data to the NWB file from the S3 bucket.
- Describe the "stimulus" `ProcessingModule` and the "timestamps" `TimeSeries` more clearly, and consider renaming/reorganizing the time series to link more clearly to the stimulus presentations.
- Remove the "imp" column from the electrodes table which contains all NaN values.
- Times in the "intervals/invalid_times" table are not in increasing order and should be to conform with NWB best practices.
- The raw LFP data is stored with gzip level 9 compression, which has a high compression ratio but is very slow to write and read. Consider using a faster compression algorithm or lower compression level, like Blosc-zstd level 5.

### Visual Coding Ophys
- The imaging plane description in the NWB 2.0 file on DANDI does not include the field of view dimensions or imaging depth. The placeholder "The imaging plane sampled by the two-photon calcium imaging at a depth of {depth} µm." was not replaced. Consider adding this information back in for clarity.
- Consider adding `start_frame` and `end_frame` columns to the stimulus presentation tables to directly index into the 2p imaging frames, to match how the AllenSDK represented these tables. Otherwise, users will need to use `np.searchsorted` on the `start_time` and `stop_time` timestamps to get these indices.


## AIND Metadata Extraction

### All Datasets
- Look into missing anesthesia duration information for some procedures.
- Look into missing maternal/paternal genotype breeding info for some subjects.

### Visual Coding Neuropixels
- Look into the subject DOB, age, and related procedures for the Visual Coding Neuropixels dataset. For example, looking at specimen ID 699733581 / mouse ID 386129, from the subject metadata we get a DOB of 2018-03-02 and from the procedures metadata we get a perfusion recorded on 2018-06-28. However, the NWB file/allensdk metadata says the data acquisition date is 2019-01-19T08:54:18Z and the age is 118D (which matches the date of the perfusion, but does not match the data of data acquisition for that animal’s supposed DOB).
- Some sessions have a probe that does not record from one of the six visual areas in the CCFv3, so we cannot select a primary targeted structure, which is required for the probe config. These probes record from non-CCF visual areas like VISmma, though.

### Visual Behavior Neuropixels
- Look into discrepancies in the session start time for some of the later sessions.