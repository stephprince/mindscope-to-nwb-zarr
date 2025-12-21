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
