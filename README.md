# mindscope-to-nwb-zarr

This repository is set up as a Code Ocean capsule to convert Mindscope NWB files from the HDF5 format to Zarr format, extract AIND Metadata JSON files, and document changes made during the conversion process.

## Running on Code Ocean as a Capsule 

The Code Ocean capsule is set up with an App Builder with a single input parameter:
- `dataset`: One of: "Visual Behavior Neuropixels", "Visual Behavior 2p", "Visual Coding Neuropixels", "Visual Coding 2p". This parameter selects which dataset to convert.

Sync the capsule with the GitHub repository, make sure the appropriate data assets have been attached to the capsule, enter the desired dataset option in the App Builder tab, and click "Run with parameters".

## Batch Conversion using Code Ocean Pipelines

### Visual Behavior Ophys




### Visual Coding Ophys

This conversion is run in a Code Ocean pipeline to parallelize over the 1518 sessions. Because the latest HDF5-based NWB 2.0 files exist on DANDI, the conversion pipeline downloads the necessary files from DANDI for each session and performs the conversion.

To run this pipeline, we need a Code Ocean data asset with one input file for each of the 1518 sessions. We will first create a Code Ocean data asset with placeholder NWB files, one per session, that tells the conversion scripts which DANDI assets to download. If this Code Ocean data asset already exists, you can skip this step.

```bash
cd code
uv run python ./scripts/create_viscod_ophys_placeholder_files.py
```

This will create 1518 empty (0 B) NWB files in the `data/visual-coding-ophys` directory.

Then, create a new Code Ocean data asset using the web interface by dragging and dropping the selection of 1518 empty NWB files from the `data/visual-coding-ophys` directory and fill out the rest of the form to create the data asset.
- Source Data Name: Visual Coding Ophys NWB HDF5 to Zarr Input
- Folder Name: data/visual-coding-ophys
- Description: Placeholder NWB HDF5 files for Visual Coding Ophys sessions to indicate which DANDI assets to download for conversion to Zarr format.
- Tags: Allen Brain Observatory
- Leave the rest blank

Then, go to the [Allen Brain Observatory Visual Coding 2p NWB HDF5 to Zarr](https://codeocean.allenneuraldynamics.org/capsule/9983566/tree) pipeline, add the new data asset to the `data` directory, and add the new data asset as an input to the pipeline. Map the paths from the data asset to the capsule (`data/visual-coding-ophys` to `capsule/data/visual-coding-ophys`), configure the capsule with the correct parameter (`--dataset Visual Coding 2p`), and connect and map the capsule to the results bucket (defaults should be fine). Finally, run the pipeline. It will take several minutes to load pipeline monitoring before starting tasks.

## Testing Locally

1. `cd code`
2. `uv run python run_capsule.py --dataset <dataset_name> --results <results_folder>`
  - `<dataset_name>`: One of: "Visual Behavior Neuropixels", "Visual Behavior 2p", "Visual Coding Neuropixels", "Visual Coding 2p"
  - `<results_folder>`: Path to a local folder where converted Zarr files and metadata will be saved. 
    - Note: By default, Windows has a maximum path length of 260 characters, which may cause issues when working with Zarr stores due to their nested directory structure. If you encounter path length issues, consider enabling long paths in Windows or changing the results folder to a location with a shorter path.
  - This command will create a virtual environment in the `code/.venv` directory and a `uv.lock` file if these do not already exist.

Note: Local testing requires access to the Mindscope NWB HDF5 files, which are not included in this repository. These files can be downloaded from the Allen Institute's data portal and must be placed in the appropriate directory structure under `data/`.


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
- Add explicit link from stimulus presentation and trials tables to the stimulus template images in the new `Images` container instead of relying on name/indices matching.

### Visual Behavior Ophys
- For multiscope sessions, name the imaging planes based on the order of the imaging planes before QC filtering so that it better matches the AIND metadata which accounts for all imaging planes.
- Remove the cached ndx-aibs-ecephys NWB extension which appears to be unused in these files.

### Visual Behavior Ephys
- Move optotagging intervals table to top-level `nwbfile.intervals`
- Set an appropriate "strain" value for the `Subject` object instead of using the placeholder "unknown".
- Rename/reorganize processing modules to be more conventional:
   - optotagging -> ogen
   - current_source_density -> ecephys


### Visual Coding Ephys
- Set an appropriate "strain" value for the `Subject` object instead of using the placeholder "unknown".
- Add stimulus template data for natural scenes, natural movies, and natural movie shuffled stimuli
