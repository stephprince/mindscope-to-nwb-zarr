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
uv run python run_capsule.py --dataset "<dataset_name>" --results_dir "<results_folder>" --metadata_only False
```

**Parameters:**
- `--dataset`: One of: `"Visual Behavior Ephys"`, `"Visual Behavior Ophys"`, `"Visual Coding Ephys"`, `"Visual Coding Ophys"` (case-insensitive)
- `--results_dir`: Path to output folder for converted Zarr files and metadata (default: `../results/`)
- `--metadata_only`: Set to `True` to generate only AIND metadata JSON files (no Zarr conversion)

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

Consider reconfiguring the pipeline to look like this [Example Hyperparameter Search Pipeline](https://codeocean.allenneuraldynamics.org/capsule/3709372/tree/v1) instead of creating input data assets.


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
| `scripts/run_all_vc_ophys.py` | Batch-generate AIND metadata for all Visual Coding 2P ophys sessions (streams from DANDI; use ≤3 workers) |
| `scripts/run_all_vc_ephys.py` | Batch-generate AIND metadata for all Visual Coding Neuropixels sessions (streams from S3; `--zip` writes one zip per session to `metadata_results/visual_coding_ephys`; use ≤3 workers) |
| `scripts/preload_vc_metadata_cache.py` | Pre-fetch subject + procedures records for Visual Coding subjects (`--dataset ophys/ephys/both`) into the shared metadata-service cache (use ≤3 workers) |


## NWB Zarr Conversion

Each conversion:
1. Reads the source HDF5 NWB file(s) for a session with `NWBHDF5IO`.
2. Applies dataset-specific transformations (see the per-dataset subsections below): updates the NWB schema and extensions, converts deprecated stimulus templates to `Images` containers, adds missing descriptions from the technical white papers, combines multi-probe/multi-plane files, rechunks large arrays, and applies dataset-specific data fixes.
3. Writes the result to Zarr with `NWBZarrIO`, producing `results/<session name>/<session name>.nwb.zarr`.
4. Optionally validates the output with nwbinspector (writing `qc/<session>.inspector_report.txt`). **This step is currently disabled for live runs** (commented out in `run_capsule.py`); the `inspect_zarr_file` helper remains available for local validation.

The per-dataset subsections below document the source data, every transformation, and all data caveats. (This content was previously split across per-dataset `CHANGELOG.md` files; it is now consolidated here so it is not missed.)

### All Datasets
- **Schema and extension updates** — the NWB schema is updated to **2.9.0** (and HDMF Common to 1.8.0 where applicable), and the in-repo NWB extensions are updated to comply: `ndx-aibs-ecephys` 0.3.0, `ndx-aibs-stimulus-template` 0.2.0, `ndx-ellipse-eye-tracking` 0.2.0, and `ndx-aibs-visual-coding-2p` 0.1.0 (each used where the dataset requires it).
- **Stimulus templates** — deprecated `StimulusTemplate`/`ImageSeries` templates are converted to modern `Images` containers with `GrayscaleImage` and `WarpedStimulusTemplateImage` objects, and `IndexSeries` references are updated to use `indexed_images` instead of `indexed_timeseries`.
- **Missing descriptions** — descriptions are added for unit metrics, trials-table columns, stimulus-presentation columns, and optogenetic-stimulation tables, sourced from the technical white papers.
- **VectorIndex dtypes** — `VectorIndex` columns are converted to `uint64` per the NWB spec.

### Visual Behavior Ephys (conversion)
**Source data** — HDF5 NWB files from `s3://visual-behavior-neuropixels-data` (`visual-behavior-neuropixels/behavior_ecephys_sessions` and `.../behavior_only_sessions`); session lists from `behavior_sessions.csv` / `ecephys_sessions.csv`. **3424 sessions** total. Behavior+ephys sessions have a base `ecephys_session_{id}.nwb` plus multiple `probe_{id}.nwb` (LFP + CSD) files; behavior-only sessions have a single `behavior_session_{id}.nwb`.

**Transformations** (schema 2.9.0 from 2.6.0-alpha)
- Combined the base session file with its probe files into a single Zarr output.
- Rechunked LFP data to `(500000, 8)` with gzip level 9 compression (~10 MB chunks) for more reliable writes to S3 from a Code Ocean pipeline.
- Added a units-table description noting that **all units are returned** (unlike Visual Coding, whose upstream data filtered out noise units).

### Visual Behavior Ophys (conversion)
**Source data** — HDF5 NWB files from `s3://visual-behavior-ophys-data` (`.../behavior_ophys_experiments` and `.../behavior_sessions`); metadata from `behavior_session_table.csv`. **4782 sessions** total: behavior-only, single-plane ophys, or multiscope ophys (up to 8 NWB files, one per imaging plane).

**Transformations** (schema 2.9.0 from 2.6.0-alpha)
- Combined multiscope sessions (multiple single-plane NWB files) into a single Zarr output, renaming per-plane objects with a `_plane_X` suffix (`imaging_plane_1`, `ophys_plane_1`, `OphysBehaviorMetadata` → `_plane_1`, etc.). `X` is 1-indexed by the experiment order in `behavior_session_table.csv`.
  - Objects duplicated across the per-plane files (stimulus table, trials, licking, …) are stored once, retaining the NWB object ID from the first experiment listed for the session.
- Set `NWBFile.session_id` = `NWBFile.identifier` so the DANDI session name more closely resembles the original HDF5 file name.

### Visual Coding Ephys (conversion)
**Source data** — HDF5 NWB files from `s3://allen-brain-observatory` under `visual-coding-neuropixels/ecephys-cache/`; session list from `.../sessions.csv`. **58 sessions** total. Each session has a base `session_{id}.nwb` (units, electrodes, session data) plus multiple `probe_{id}_lfp.nwb` files (LFP + CSD per probe).

**Transformations** (schema 2.9.0 from 2.2.2, HDMF Common 1.8.0 from 1.1.3; extension ndx-aibs-ecephys 0.3.0)
- Combined the base session file with its probe LFP files into a single Zarr output.
- Rechunked LFP data to `(500000, 8)` with gzip level 9 compression (~10 MB chunks).
- Added CSD data from the probe files into a newly created `ecephys` processing module under unique names `probe_{probe_id}_ecephys_csd`.
- **Added AllenSDK per-unit visual-response analysis metrics to the `units` table** — receptive-field, tuning (orientation/direction/spatial-frequency/temporal-frequency/phase), running modulation, per-stimulus firing rates, lifetime sparseness, image selectivity, etc. These are published by the Allen Brain Observatory as static CSVs (`{brain_observatory_1.1,functional_connectivity}_analysis_metrics.csv`) and are **absent from the source NWB**; they are streamed from S3 at conversion time (no AllenSDK dependency) and attached by `_units_analysis_metrics.py`. Column set is session-type-dependent: **57 metrics** for `brain_observatory_1.1`, **45** for `functional_connectivity` (which adds dot-motion `*_dm` metrics and omits static-gratings/natural-scene metrics).
- **Added numeric `ecephys_structure_id` to the `electrodes` table** (from the published `channels.csv`) — the numeric Allen CCFv3 counterpart of the existing `location` acronym.

**Data caveats / session subsets**
- **Analysis metrics are per-unit and not universal.** A small per-session fraction of units have no metrics row and get **NaN** across all metric columns (e.g. session 715093703: 2714/2779 units matched, 65 NaN; session 766640955: all 2890 matched). Metrics are present for most units **including `quality == "noise"` units**, not just AllenSDK's default good/in-brain set. Within a matched unit, individual metrics can also be NaN where undefined. `ecephys_structure_id` is NaN for out-of-brain/unassigned channels. The archived `units` table keeps **all** units (unfiltered); to reproduce AllenSDK's default `session.units` filter, select `quality=='good'` & structure not null & `amplitude_cutoff<=0.1` & `presence_ratio>=0.95` & `isi_violations<=0.5`.
- **Base-only session (no LFP): 839557629.** All 5 probes on this `functional_connectivity` session are flagged `has_lfp_data=False`, so no `probe_*_lfp.nwb` files exist (on S3 or DANDI:000022) and the Zarr contains **no LFP or CSD**. The spikes/units (1770 units), electrodes, and stimulus data are intact. The conversion detects the absence of probe files and writes a base-only Zarr (no `ecephys` module); it also completes quickly, since the per-probe LFP files are the bulk of the data.
- The source `filtering` electrodes column is declared `float32` but stores strings (invalid under the old schema); it round-trips to Zarr as strings without error or loss, so no special handling is applied.

### Visual Coding Ophys (conversion)
**Source data** — HDF5 NWB 2.0 files from the DANDI Archive, dandiset **000728**, version 0.240827.1809 (themselves converted from the original NWB 1.0 Brain Observatory files by [catalystneuro/visual-coding-to-nwb-v2](https://github.com/catalystneuro/visual-coding-to-nwb-v2) in Aug 2024). **1518 sessions**, each with two HDF5 files: one with session metadata + processed 2p data, one with raw 2p imaging. Experiment metadata comes from `s3://allen-brain-observatory/visual-coding-2p/ophys_experiments.json`.

**Transformations** (schema 2.9.0 from 2.7.0; extension ndx-aibs-visual-coding-2p 0.1.0)
- Combined the processed NWB file (metadata + processed 2p data) with the raw NWB file (raw 2p imaging) into a single Zarr output. **TEMPORARY: raw 2p data is currently excluded from the export** — the raw download and the raw-acquisition merge are commented out in `run_conversion.py` (see the matching `NOTE` markers) to be re-enabled later; the exported Zarr presently contains only the processed data.
- Changed the subject ID to the external donor name (6-digit `external_donor_name`) from the experiment metadata; files are named `sub-<donor_name>_ses-<experiment_id>_behavior+image+ophys.zarr`.
- Added the `ndx-aibs-visual-coding-2p` extension and an `OphysExperimentMetadata` (`LabMetaData`) object carrying AllenSDK experiment metadata absent from the source NWBs.
- Converted natural-movie `ImageSeries` templates (which had NaN rate/starting time) to `Images` containers of `GrayscaleImage` frames, updating the presentation `IndexSeries` to reference them.
- Added `order_of_images` to existing `Images` containers (natural scenes, locally sparse noise), ordered by numeric suffix.
- Rechunked the raw 2p `MotionCorrectedTwoPhotonSeries` to chunk shape `(75, 512, width)` to reduce chunk count for cloud storage (S3 COPY-rate) limits.

**Data caveats / session subsets** — three fixes below repair or augment the source data, each on a subset of sessions:
- **`static_gratings` truncated to 3 rows (upstream bug, fixed in this conversion; 506 sessions).** Every DANDI (v2) `static_gratings` stimulus presentation table (under `nwb.stimulus`) holds only 3 rows instead of the full ~6000-presentation block. The cause is a bug in the upstream converter `catalystneuro/visual-coding-to-nwb-v2` ([issue #49](https://github.com/catalystneuro/visual-coding-to-nwb-v2/issues/49)): its `interfaces/_static_grating_stimulus.py` computes the blank-sweep mask with **row** indexing (`nan[0] & nan[1] & nan[2]`) instead of **column** indexing (`nan[:,0] & nan[:,1] & nan[:,2]`), so the mask has length 3 and the `zip` that builds the `TimeIntervals` truncates the table to 3 rows. (`three_session_B` is the only session type with `static_gratings`; 506 sessions are affected.)
  - **Fix in this repo:** the full table is rebuilt from the authoritative AllenSDK `get_stimulus_table("static_gratings")`. Because AllenSDK needs an incompatible dependency stack (pynwb 2.x) versus the conversion env (pynwb 4.1.0), it is run in a **separate environment** and its output is **cached to disk** first: `code/scripts/extract_vc_ophys_static_gratings.py` streams each v1 Brain Observatory NWB from S3 and writes one CSV per experiment id to `data/visual-coding-ophys-static-gratings/` (a mounted data asset on Code Ocean). During conversion, `run_conversion.py`'s `rebuild_static_gratings_from_cache` drops the truncated 3-row table and attaches the full one (e.g. session 645474010 goes from 3 → 6000 rows, 191 blank sweeps), preserving the same column names/descriptions.
- **Epochs table reconstructed (34 sessions).** 34 of the 1518 sessions have no `epochs` intervals table in the DANDI source (the entire `intervals` group is absent). The conversion reconstructs it from `nwb.stimulus` in the time domain (`pynwb_utils.reconstruct_stimulus_epochs_table`, after the `static_gratings` rebuild) and sets `nwbfile.epochs`. This is fail-loud and runs in both pipelines; see the detailed **"Missing epochs table in 34 sessions"** note under *AIND Metadata Extraction → Visual Coding Ophys* for the root cause and validation.
- **Eye tracking added (2p-frame-aligned; the only eye-tracking product stored; 818 sessions).** The conversion stores exactly one eye-tracking product: the `eye_tracking.npy` served by `BrainObservatoryCache.get_eye_tracking()` (columns documented by AllenSDK PR #2740: `[2p_frame_number, eye_area_cm², pupil_area_cm², azimuth_deg, altitude_deg]`), present for **818 of 1518** experiments. Provenance: these files were **generated by the [`AllenInstitute/visual-coding-saccades`](https://github.com/AllenInstitute/visual-coding-saccades) project** (`data_loading/package_eye_data.py`) from Allen LIMS raw eye-tracking + time-sync files, resampled to the 2-photon frames, and published in the versioned `visual-coding-ophys-data` S3 release (a us-west-2, S3CloudCache-managed bucket). `run_conversion.py` → `_eye_tracking_2p.py::add_eye_tracking_2p` downloads it env-free via quilt3 (no AllenSDK) and attaches to `nwb.processing['behavior']` under the **standard container names**, timestamped by the session's dF/F (2-photon) frame clock:
  - `CompassDirection` — `SpatialSeries` `pupil_location_spherical` (pupil gaze **azimuth** (horizontal) / **altitude** (vertical), **degrees**);
  - `PupilTracking` — `TimeSeries` `pupil_area` and `eye_area` (**cm²**).
  The frame-index column indexes the dF/F frames, so no separate timestamp column is stored. **When this product is added, the DANDI file's v1-embedded eye tracking (`EyeTracking`/`PupilTracking`/`CompassDirection`) is removed first**, so those standard names are free and the file carries a single eye-tracking product. Because the container names are reused, **each series `description` states the source** (the `visual-coding-saccades` / `get_eye_tracking` product) so the object is self-describing. For the ~700 sessions not in the release this is a no-op and any v1-embedded eye tracking is left as-is (see the metadata note below). Allen flags this DLC product as **provisional** ("new eye tracking algorithm … in the process of being validated"); each series description records the provenance and that caveat.
- **A second DLC product (`ophys_eye_gaze_mapping`) exists but is deliberately NOT included.** For the record: the Allen Brain Observatory also publishes a gaze-mapping product at `s3://allen-brain-observatory/visual-coding-2p/ophys_eye_gaze_mapping/<experiment_id>_<ophys_session_id>_eyetracking_dlc_to_screen_mapping.h5` (~20 MB, **837** experiments), containing `eye_area`/`pupil_area` (**pixels²**), `screen_coordinates` (**cm**) and `screen_coordinates_spherical` (**degrees**), each in a **blink-filtered** (`new_*`, ~43% NaN) and an **unfiltered** (`raw_*`) variant, at the **eye-camera** frame rate with `synced_frame_timestamps`. It is the same underlying DeepLabCut tracking as the product above (pupil areas correlate **0.998**, up to a px²→cm² conversion) but at a different (eye-camera) rate and with a **swapped gaze-axis convention** (its `y_pos_deg` is horizontal/azimuth, `x_pos_deg` is vertical/altitude — so its narrow `x_pos_deg` range is the low-variance vertical axis, not a defect). **This repo does not ingest it** — only the 2p-frame-aligned `get_eye_tracking` product above is stored.

**Code Ocean run model.** The conversion is a Code Ocean *pipeline* where each parallel job mounts exactly **one** metadata zip from the `visual-coding-ophys-metadata-only` data asset (the 1518 zips from `run_all_vc_ophys.py --zip`), unzips it, reads the ophys experiment id from `data_description.json`, downloads that session's processed + raw NWBs from DANDI, applies all the fixes above, and writes `results/<session name>/<session name>.nwb.zarr`. The module constant **`TEST_ONLY_ZIP_NAME`** in `run_conversion.py` can gate this for validation: when set to a zip filename, only the job whose mounted zip matches does work and every other job is a no-op (empty placeholder). It is **`None`** (production), so every job converts its mounted zip and all 1518 sessions are processed.


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

(The `static_gratings` rebuild, 2p-frame-aligned eye tracking, the excluded `ophys_eye_gaze_mapping` product, epochs reconstruction, and the Code Ocean run model are implemented fixes, documented under *NWB Zarr Conversion → Visual Coding Ophys (conversion)* above — not future work.)


## AIND Metadata Extraction

### All Datasets
- The `BEHAVIOR_VIDEOS` modality is always included in the data stream and data description modalities (added unconditionally in `get_modalities`). Behavior videos (eye + body cameras) were recorded for every Allen Brain Observatory experiment, even when that camera data is not packaged in the NWB files.
- Look into missing anesthesia duration information for some procedures.
- Look into missing maternal/paternal genotype breeding info for some subjects.

### Visual Coding Neuropixels

**Batch metadata generation.** `code/scripts/run_all_vc_ephys.py` generates the full AIND metadata set for the Visual Coding Neuropixels sessions, streaming each session NWB from the public `allen-brain-observatory` S3 bucket over HTTPS (no download / no mount) and writing the five-file set (data description, subject, acquisition, procedures, instrument) per session. The cohort is 58 unique mice — 32 `brain_observatory_1.1` and 26 `functional_connectivity` sessions. Stimulus epochs are emitted **one per contiguous stimulus block** (grouped by the `stimulus_block` column of each presentation table; the `spontaneous` table, which has no such column, yields one epoch per row) plus one `Optotagging` epoch — not one per stimulus table — matching the Visual Coding ophys pipeline. `functional_connectivity` sessions are uniformly **17 epochs**; `brain_observatory_1.1` sessions are **typically 31 (30–33 across the cohort — the count varies with the number of spontaneous blocks)**. The 31-epoch case breaks down as 3 drifting-gratings, 3 static-gratings, 3 natural-scenes, 2 natural-movie-one, 2 natural-movie-three, 1 gabors, 1 flashes, 15 spontaneous, and 1 optotagging. Use **≤3 workers**: every ephys subject hits the slow metadata-service raw-parse fallback (whose `procedures` endpoint also returns transient HTTP 500s under concurrency, recovered by per-session retry), and each worker also streams a ~2.6 GB NWB and reads its spike times (for the acquisition end time).

Pass **`--zip`** to bundle each session's five files into a single `<data asset name>.zip` in `code/metadata_results/visual_coding_ephys/` (the run report is written to `code/metadata_results/reports/visual_coding_ephys/`, a sibling directory, so the deliverable directory holds only the 58 per-session zips).

The following warnings are expected and handled:

- **Metadata-service reconciliation** — when a subject or procedures record fails client-side schema validation, the raw service response is parsed and known gaps are patched: genotype is **backfilled from the NWB** (`"wt/wt"`) for wildtype subjects whose LIMS genotype is null (see below); a null `maternal_genotype`/`paternal_genotype` within a present `breeding_info` is set to `""`; and `functional_connectivity` procedures get the **anaesthesia-duration** (`0.0`) and **Craniotomy-position** (`'Left'` → `["Left"]`) fixes.
- **Per-block stimulus epochs (all 58 sessions)** — `get_stimulation_epochs` splits each presentation table into its contiguous blocks (by `stimulus_block`) and emits one `StimulusEpoch` per block, each with its own start/stop, so a stimulus that recurs in non-contiguous blocks no longer collapses into a single over-broad span. A sanity-check warning fires only if the resulting per-block epochs unexpectedly overlap in time (blocks are disjoint by construction, so it should not fire). The per-block `StimulusEpoch` carries no `training_protocol_name`/`curriculum_status` (those are behavior-only fields; the ephys `session_type` is recorded as the acquisition `acquisition_type`), and lists the `Stimulus Screen` monitor as its active device.
- **Deprecated coordinate-system warnings (upstream `aind-data-schema`)** — model serialization emits several `DeprecationWarning`s; all are upstream schema-migration noise rather than data errors:
  - `CoordinateSystem 'PROBE_RUFD' uses a DEPTH axis, which is deprecated` — the probe-local `PROBE_RUFD` system defines a 4th `DEPTH` axis (the reference probe transforms encode insertion depth as a 4-element translation tied to it), whereas `aind-data-schema` now prefers a standard 3-axis system with Z as depth. This is the only one partly ours; moving to a 3-axis system would change how insertion depth is represented in the transform, so it is deferred pending sign-off from the reference-geometry author.
  - `Deprecated: use local_coordinate_system instead` / `use global_coordinate_system instead` — Pydantic warns when the legacy `coordinate_system` field (still present on `aind-data-schema`'s config/device/procedure models) is accessed during serialization. Only the new `local_coordinate_system`/`global_coordinate_system` fields are populated, so the data is correct; the warning goes away once upstream drops the deprecated field.
  - `'coordinate_system' is deprecated. Please use 'global_coordinate_system' instead` — `aind-data-schema`'s `mode="before"` migrator warns when a serialized model still carries the old key without the new one during the dump→validate round-trip.
- **Reproducibility note** — `Neither commit_hash nor version provided for Code`, once per stimulus/optotagging `Code` block; the Visual Coding stimulus code has no recorded version.

**Cross-checks (warn-only, LIMS kept).** The subject is cross-checked against LIMS for **sex**, **genotype** (when both sources have a value), **date of birth** (>2-day tolerance), **species**, and **mouse id**. A **species** mismatch hard-fails the session; the others warn and keep the LIMS value. An unreachable metadata service also warns and yields no service-derived files for that session.

- **Optotagging parameters — partly whitepaper-sourced, cross-checked where possible.** The per-pulse durations and light levels come from the NWB optotagging table, but the inter-pulse interval and raised-cosine ramp duration are not recorded there and are taken from the technical whitepaper. `verify_optostimulation_timing` cross-checks the whitepaper inter-pulse interval against the observed median pulse-onset gap and warns on a gross mismatch; the ramp duration has no counterpart in the file and cannot be verified.
- Look into the subject DOB, age, and related procedures for the Visual Coding Neuropixels dataset. For example, looking at specimen ID 699733581 / mouse ID 386129, from the subject metadata we get a DOB of 2018-03-02 and from the procedures metadata we get a perfusion recorded on 2018-06-28. However, the NWB file/allensdk metadata says the data acquisition date is 2019-01-19T08:54:18Z and the age is 118D (which matches the date of the perfusion, but does not match the data of data acquisition for that animal’s supposed DOB).
- Some sessions have a probe that does not record from one of the six visual areas in the CCFv3, so we cannot select a primary targeted structure, which is required for the probe config. These probes record from non-CCF visual areas like VISmma, though.
- The experiment start time, rig ID, and operator ID for the acquisition metadata come from `code/reference/neuropixels_vc_experiment_metadata.csv` (transcribed from the original platform JSON files), because the NWB `session_start_time` is a packaging date rather than the true acquisition time (e.g., session 715093703 reads 2019-01-19 in the NWB file but the experiment actually ran 2018-06-27, ~205 days earlier). NWB-derived stream and stimulus-epoch times are re-anchored to the CSV start time. The rig ID (`"NP.1"`/`"NP.2"`) sets the acquisition `instrument_id` and the generated `Instrument.instrument_id`, so they match. One session, **819701982**, is missing from this CSV, so it falls back to the NWB `session_start_time` (the packaging date), the fallback instrument ID `"NP"`, and no operator; this fallback is recorded in the acquisition `notes` for that session. Its experiment start time, rig ID, and operator should be filled in if the platform JSON becomes available.
- **Session 750749662 — duplicated platform JSON, date corrected in code.** The platform JSON transcribed for session **750749662** was a duplicate of session **750332458**'s, so 750749662's `ExperimentStartTime`/`ExperimentCompleteTime` in `reference/neuropixels_vc_experiment_metadata.csv` carry 750332458's date (2018-09-10). The source filename (`750749662_412792_20180911_platformD1.json`) and the true acquisition put it on **2018-09-11**, with the time-of-day (~14:03 start, ~16:45 end) taken as roughly correct. The reference CSV is left untouched; `get_experiment_metadata` special-cases this session and corrects the date to 2018-09-11 on read (keeping the times), so the acquisition start, the acquisition end-time check, the data-description asset name, and the subject DOB anchor all use the corrected date. The values for 750332458 are unaffected and correct. If the correct platform JSON for 750749662 becomes available, this special case can be removed and the CSV updated.
- **Session 840012044 — wrong platform JSON, start time corrected in code.** Session **840012044**'s reference CSV row came from the wrong platform JSON: its `ExperimentStartTime` (`2019-03-21T16:34:26`) is only ~2 minutes before its `ExperimentCompleteTime` (`2019-03-21T16:36:42`), whereas real sessions run ~160–178 min. The end is roughly correct, so the start is moved earlier. The correction is derived from the NWB itself: the session's data span (`get_latest_time` = 165.5 min) sets `ExperimentStartTime` = end − 165.5 min = **`2019-03-21T13:51:15`**, which also makes the pipeline's computed acquisition end land on the recorded `ExperimentCompleteTime`. The reference CSV is left untouched; `get_experiment_metadata` special-cases this session on read. If the correct platform JSON becomes available, this special case can be removed and the CSV updated.
- **Optogenetics rig differs by session (LED vs laser).** The optotagging light source in the generated `Instrument` and the optotagging epoch config switches by session id: sessions with id **≥ 789848216** use a **473 nm laser**, earlier sessions a **465 nm LED**. This changes the instrument components, the instrument `modification_date`, and the optotagging epoch's active devices. (The two `modification_date` values — `2018-09-25` for the LED build, `2019-01-08` for the laser — are synthetic stand-ins, and the instrument carries a `notes` field stating it was reconstructed posthoc from incomplete records.)
- **Stimulus presentation tables are complete (no truncation).** Unlike the Visual Coding **ophys** `static_gratings` tables (truncated upstream, see the conversion section), the Neuropixels stimulus presentation tables are full in the source, so the metadata's per-block epoch counts and per-presentation parameters are complete for every session.
- **Session 839557629 — base-only Zarr (no LFP).** This affects only the Zarr **conversion** output (no LFP/CSD; see *NWB Zarr Conversion → Visual Coding Ephys*); metadata generation treats 839557629 as an ordinary `functional_connectivity` session (17 epochs, standard procedures fixes).
- **Genotype null in LIMS — backfilled from the NWB.** The AIND metadata service (LIMS) returns a **null `genotype`** for about half the Visual Coding Neuropixels subjects (30 of the 57 present in `reference/neuropixels_vc_experiment_metadata.csv`), all of them wildtype animals; those same records also have a null `breeding_info`. The NWB files, however, do record a genotype for every one of these subjects — e.g. LIMS `null` vs NWB `"wt/wt"`. Because the value is not actually missing, only absent from LIMS, `fetch_subject_from_aind_metadata_service` **backfills the genotype from the NWB file** when the LIMS value is null (`genotype` is a required `str` in the schema) and warns per subject. It **fails loudly** (raises) only if the genotype is missing from *both* LIMS and the NWB. When both sources have a value but they differ (the known short-form vs full-allelic-form notation difference), LIMS is kept and a warning is emitted, consistent with the sex/DOB cross-checks. This NWB backfill is specific to the top-level `genotype` and is distinct from the `""` ("unknown") stand-in used for a null `maternal_genotype`/`paternal_genotype` *within* a present `breeding_info`. A null `breeding_info` itself is schema-valid and is left as-is.

### Visual Coding Ophys

**Batch metadata generation.** AIND metadata is generated for **all 1518** Visual Coding 2P ophys experiments (DANDI dandiset 000728), streamed from DANDI (no download), by `code/scripts/run_all_vc_ophys.py`. Every session produces the full five-file set (data description, subject, acquisition, procedures, instrument). Pass **`--zip`** to bundle each session's five files into a single `<data asset name>.zip` in `code/metadata_results/visual-coding-ophys-metadata-only/` — one zip per session, nothing else (the run report stays under `scratch/`). That directory name matches the conversion's data-asset mount (`data/visual-coding-ophys-metadata-only/`), so the generated zips are uploaded as-is to drive the Zarr conversion.

Worker count depends on the metadata service: a **fresh** run must use **≤3 workers** (the AIND metadata service returns empty `procedures` bodies under higher concurrency → `JSONDecodeError`). Once every subject's subject+procedures responses are cached on disk (`code/scratch/aind_metadata_cache/`), the run is **fully offline** and needs no VPN — all 271 ophys subjects are cached, so a full regeneration runs at higher concurrency (e.g. 6 workers, ~79 min). The batch is resumable (keyed on experiment id in `sessions.jsonl`); a completeness gate rejects partial output, so failures are retryable, never silent. The **34 sessions that previously had 0 stimulus epochs are now fixed** — every session carries reconstructed epochs (see the "Missing epochs table" note above).

Latest full run (2026-08-13): **1518/1518 OK, 0 errors**, in ~79 min. Every session's five files **validate** against `aind-data-schema` 2.9.0 (each loads into its core model — `DataDescription`/`Subject`/`Procedures`/`Instrument`/`Acquisition`). The run emitted **61,390 warnings**, all of the expected/benign types below (0 unexpected): mostly the per-`Code` "no commit_hash/version" note (~37k) and upstream `aind-data-schema` deprecations (`coordinate_system`→global/local ~18k, `Device.manufacturer` ~4.6k); plus 1,518 `static_gratings`-truncation notes (506 StimB sessions × 3 blocks — the metadata reads the truncated DANDI table; only the Zarr rebuilds it), and 41 sex + 71 genotype LIMS-vs-NWB reconciliations (LIMS kept).

The following warnings are expected and hold across the dataset:

- **AIND schema deprecations** (every session) — `aind-data-schema` emits `DeprecationWarning`s as `coordinate_system` is migrated to `global_coordinate_system` / `local_coordinate_system`; pynwb/hdmf also emits a deprecated-`Device.manufacturer` warning while reading each NWB. Benign upstream schema churn.
- **Reproducibility note** (nearly every session) — `Neither commit_hash nor version provided for Code`, emitted by `aind-data-schema` once per stimulus `Code` block. The Visual Coding stimulus code has no recorded version, so this is expected.
- **Null breeding genotypes fixed** — `maternal_genotype` and/or `paternal_genotype` are null in most service records but are required schema fields, so they are set to `""` ("unknown"); the fix is logged per subject. Most subjects also hit a subject-record validation fallback (the raw service response is parsed), which is expected and drives these fixes.
- **Checks that never triggered in this dataset** — across all 1518 sessions, none required the **missing anaesthesia duration** fix or the **Craniotomy position type** fix (see the procedures patches below), and none produced a **date-of-birth difference** warning (every LIMS DOB agrees with the NWB-derived DOB within the ±2-day tolerance). These handlers exist but were not exercised here.

**NWB-vs-LIMS subject mismatches.** LIMS is authoritative; the pipeline keeps the LIMS value and warns. Subject IDs (6-digit mouse / `external_donor_name`) with one example DANDI NWB file for lookup:

*Sex mismatch — NWB records Male, LIMS records Female:*

| subject | example NWB file |
|---|---|
| 232269 | `sub-501800347_ses-509959266-StimB_behavior+image+ophys.nwb` |
| 232270 | `sub-501800590_ses-510234687-StimA_behavior+image+ophys.nwb` |
| 243303 | `sub-516024042_ses-530026508-StimC_behavior+image+ophys.nwb` |
| 252106 | `sub-521948687_ses-531346896-StimC_behavior+image+ophys.nwb` |

*Genotype mismatch — NWB short form vs LIMS full allelic form (same transgenes). Subjects 222426, 229109, 233215 differ only by trailing whitespace in the LIMS value:*

| subject | example NWB file |
|---|---|
| 222426 | `sub-495727015_ses-502793808-StimA_behavior+image+ophys.nwb` |
| 229109 | `sub-501228870_ses-504809131-StimC_behavior+image+ophys.nwb` |
| 232269 | `sub-501800347_ses-509959266-StimB_behavior+image+ophys.nwb` |
| 232270 | `sub-501800590_ses-510234687-StimA_behavior+image+ophys.nwb` |
| 233215 | `sub-503292442_ses-512326618-StimA_behavior+image+ophys.nwb` |
| 243303 | `sub-516024042_ses-530026508-StimC_behavior+image+ophys.nwb` |
| 252105 | `sub-522150294_ses-531126287-StimB_behavior+image+ophys.nwb` |
| 252106 | `sub-521948687_ses-531346896-StimC_behavior+image+ophys.nwb` |

**Missing epochs table in 34 sessions (reconstructed).** 34 of the 1518 sessions have **no** `epochs` intervals table — in fact the entire `intervals` group (`epochs`, `trials`, `invalid_times`) is absent (`nwbfile.epochs is None`, `nwbfile.intervals` empty). The 34 span all four types (`three_session_A` ×21, `three_session_C2` ×8, `three_session_C` ×3, `three_session_B` ×2).
  - **Root cause.** The Allen Brain Observatory v1 NWBs store an *empty* `epoch` group; the real epoch table is **computed** by the AllenSDK (`BrainObservatoryNwbDataSet.get_stimulus_epoch_table` → `get_epoch_mask_list`), which splits each stimulus's presentations into blocks wherever the inter-presentation frame gap exceeds a session-type threshold and **raises `EpochSeparationException` if a stimulus splits into >3 blocks** (`max_cuts=2`). The upstream converter (`catalystneuro/visual-coding-to-nwb-v2`, `scripts/generate_epoch_tables.py`) precomputed these tables and `except EpochSeparationException: continue` — so any session that raised got no epoch JSON and hence no `epochs` group. Each of the 34 has exactly one stimulus with a **spurious sub-second dropped-frame gap barely over threshold** (e.g. drifting_gratings gaps `[29903, 28092, 44]` frames — two real ~15-min gaps plus one 44-frame artifact just over the 39-frame threshold), creating a 4th block that trips the guard.
  - **Fix in this repo.** The epochs table is **reconstructed** from `nwb.stimulus` in the time domain by `pynwb_utils.reconstruct_stimulus_epochs_table`: each stimulus's presentation onsets/offsets (TimeIntervals `start/stop`; IndexSeries/TimeSeries `timestamps`) are split into blocks on inter-presentation gaps > 10 s (the real block gaps are ~900 s; the artifacts < ~1.5 s, so the threshold has a ~3-orders-of-magnitude margin). `static_gratings` is read from its corrected cache (it is truncated in the source, see above). The function **fails loudly** (raises, never emits a bad table) if any stimulus yields >3 blocks, a block is implausibly short or overlapping, or — when a stored epochs table exists — the reconstruction disagrees with it. Validated to reproduce the stored epochs exactly on good sessions. This runs in **both** pipelines: the metadata generator populates `acquisition.json`'s `stimulus_epochs` for these 34, and `run_conversion.py` sets `nwbfile.epochs` on the Zarr (after the static_gratings rebuild).

Subject and procedures metadata are fetched from the AIND metadata service by 6-digit mouse ID (`external_donor_name`). Some legacy records do not validate against the current schema, so the raw service response is parsed and known data issues are patched during generation:
- **Missing breeding genotypes** — `maternal_genotype` and/or `paternal_genotype` are sometimes null in the service record, but both are required (`str`) fields in the schema (confirmed still required as of `aind-data-schema` 2.8.1). When null, they are set to `""` — an empty string standing in for "unknown", not a real (empty) genotype.
- **Missing anaesthesia duration** — a Surgery whose `anaesthesia` has no `duration` is given `duration = 0.0`.
- **Craniotomy position type** — a Craniotomy `position` stored as a string is wrapped in a list, which the schema expects.
- **Subject check** — the subject metadata is taken from the AIND metadata service (LIMS), which is treated as authoritative, and cross-checked against the NWB file. A **species** mismatch raises and fails the whole session (this must never differ). A **sex** mismatch, a **date-of-birth** difference beyond a ±2-day tolerance, or a **genotype** mismatch only **warns** and proceeds with the LIMS value, because the NWB files and LIMS are known to disagree for some subjects. The NWB stores only an integer-day age (`P<days>D`), so the DOB derived from it (acquisition date − age) is approximate; the LIMS DOB is used. Note: unlike the Visual Coding **Neuropixels** pipeline, the ophys pipeline does **not** backfill the top-level `genotype` from the NWB when LIMS is null — it keeps the LIMS value and only warns on a genotype mismatch. The `""` ("unknown") stand-in is applied solely to null `maternal_genotype`/`paternal_genotype`.
  - **Known NWB-vs-LIMS subject discrepancy** — for subject **232269** (session 509292861), the sex is recorded as **male** in *both* the original v1 NWB (`s3://allen-brain-observatory/visual-coding-2p/ophys_experiment_data/509292861.nwb`) and the DANDI v2 NWB, but as **female** in *both* the AllenSDK metadata and the AIND service (LIMS). The age also differs (v1 NWB `78 days` vs LIMS `~97 days`/`P97`), and the genotype is recorded in short form in the NWB (`Rbp4-Cre; Camk2a-tTA; Ai93(TITL-GCaMP6f)`) vs the full allelic form in LIMS (`Rbp4-Cre_KL100/wt;Camk2a-tTA/wt;Ai93(TITL-GCaMP6f)/Ai93(TITL-GCaMP6f)-STOPdel`) — a notation difference for the same transgenes. The sex/age split is 2-vs-2 between the NWB files and the metadata databases; which is correct requires the authoritative animal record. The pipeline follows LIMS (emits female, LIMS DOB and genotype) and warns on each mismatch. Other subjects may have similar mismatches.

Other per-session handling:
- **Stimulus epochs** — the session's `epochs` intervals table is emitted as one `StimulusEpoch` per block (per row), each with its own start/stop from the NWB. Each epoch is annotated from the matching `nwb.stimulus` object: its description (as `notes`), the referenced template name for natural scenes/movies, a presentation/frame count within the block window, and any per-presentation parameter columns (e.g. grating orientation, spatial frequency, phase). Note: this annotation reads the DANDI source `nwb.stimulus` directly, so for `static_gratings` it sees the upstream-truncated 3-row table (see the `static_gratings` caveat above) — only the first static-gratings block carries per-presentation parameters. The Zarr conversion's `static_gratings` fix (rebuilding from the AllenSDK cache) is applied during conversion and does not feed back into this metadata annotation.
- **Instrument (rig) resolution** — the rig (CAM2P.1–CAM2P.5) is resolved by `ophys_experiment_id` (the pipeline's iteration key, equal to `nwbfile.session_id`) from `reference/ophys_session_experiment_screen_centers.csv`, and the original/final rig configuration is selected by acquisition date. All 1518 experiments resolve to CAM2P.1–CAM2P.5, which all have instrument definitions, so an instrument file is written for every session. If an experiment resolves to a rig without an instrument definition (e.g., CAM2P.6, MESO.1), `generate_instrument` raises. The same CSV supplies the `ophys_session_id` tag on the data description.
- **Acquisition start time** — taken directly from the NWB `session_start_time`. Unlike the Visual Coding **Neuropixels** pipeline (where `session_start_time` is a packaging date that is re-anchored from a reference CSV), the ophys `session_start_time` is the true acquisition time, so no CSV re-anchoring and no packaging-date `notes` caveat are applied.
- **Unrecorded laser power** — the two-photon laser power was adjusted per session and not recorded in the NWB, so it is a placeholder in the imaging config: `LaserConfig.power` is left `None` (optional), while `Plane.power` (a required field) is set to the sentinel `-1` percent to signal "unknown".
- **Unrecorded emission wavelength** — the imaging plane's `emission_lambda` is stored as `NaN` for every session (only the 910 nm `excitation_lambda` is recorded), so the channel's `emission_wavelength` is left `None`.
- **Eye/pupil tracking is present for a subset; camera videos are not.** (This is about the **converted Zarr** output — the generated metadata is uniform across all 1518 sessions: the instrument always declares Eye + Body `CameraAssembly`s and the acquisition always lists them as active devices, regardless of which eye-tracking product a session has. See the eye-tracking note under *NWB Zarr Conversion → Visual Coding Ophys* for the primary description.) The DANDI NWBs already carry a **v1-embedded** eye tracking for the sessions whose v1 source NWB had a `processing/brain_observatory_pipeline/EyeTracking` group (**363 of 1518**): the upstream converter copied it into `processing['behavior']` as `EyeTracking` (`SpatialSeries` `pupil_location`, unit m), `CompassDirection` (`pupil_location_spherical`, degrees), and `PupilTracking` (`pupil_size`, px), referenced to the monitor center. This repo's conversion stores the 2p-frame-aligned `get_eye_tracking` product for **818** sessions and, **when it does, removes that v1-embedded eye tracking** (see the eye-tracking note under **Visual Coding Ophys** conversion above), so those sessions carry a single product — **177** of the 363 v1-embedded sessions overlap the release and have their v1 data replaced. The other **186** v1-embedded sessions are **not** in the `get_eye_tracking` release and keep their v1-embedded eye tracking unchanged. What is still **not** packaged, for any session, is the eye/body **camera video**, even though the cameras are described in the instrument.

### Visual Behavior Ophys

Metadata is generated for the Visual Behavior 2P dataset — behavior-only sessions plus single-plane (CAM2P) and mesoscope (MESO) ophys sessions (one NWB per imaging plane).

- **Imaging frame rate** — the per-plane `imaging_rate` from the NWB imaging plane flows through to the acquisition's `ImagingConfig.SamplingStrategy.frame_rate`. Single-plane (CAM2P) sessions are 31 Hz; mesoscope (MESO) sessions are asserted to be one of {5, 6, 9, 11} Hz. A survey of all 265 mesoscope sessions found **11 Hz is the standard at every plane count (252 of 265 sessions)**; the **13 sessions below run slower** (9/6/5 Hz), which is *not* explained by plane count alone (they occur at 2/3/6/7 planes):

  | ophys_session_id | imaging_rate (Hz) | planes | plane groups | project_code |
  |---|---|---|---|---|
  | 962045676 | 5 | 7 | 4 | VisualBehaviorMultiscope4areasx2d |
  | 873720614 | 6 | 2 | 1 | VisualBehaviorMultiscope |
  | 1048363441 | 9 | 6 | 4 | VisualBehaviorMultiscope |
  | 1049240847 | 9 | 6 | 4 | VisualBehaviorMultiscope |
  | 1050231786 | 9 | 6 | 4 | VisualBehaviorMultiscope |
  | 1050597678 | 9 | 6 | 4 | VisualBehaviorMultiscope |
  | 1051107431 | 9 | 3 | 2 | VisualBehaviorMultiscope |
  | 1051319542 | 9 | 2 | 1 | VisualBehaviorMultiscope |
  | 1052096166 | 9 | 3 | 2 | VisualBehaviorMultiscope |
  | 1052330675 | 9 | 2 | 2 | VisualBehaviorMultiscope |
  | 1052512524 | 9 | 3 | 2 | VisualBehaviorMultiscope |
  | 1056065360 | 9 | 2 | 2 | VisualBehaviorMultiscope |
  | 1056238781 | 9 | 3 | 2 | VisualBehaviorMultiscope |

### Visual Behavior Neuropixels

**Stimulus epochs.** `get_stimulation_epochs` emits **one epoch per contiguous `stimulus_block`** (grouped by the `stimulus_block` column of each presentation table), matching the Visual Coding Neuropixels pipeline, with one Visual-Behavior addition: the change-detection **behavior task** — the presentation table's `active == True` rows (the `Natural_Images_*` table on ecephys sessions, or `grating_presentations` on early training) — is emitted as a single `Change detection - Active` epoch that carries the session's **`training_protocol_name`** (`session_type`) and **`curriculum_status`**. The passive replay (`active == False`) and the passive mapping stimuli (flash / gabor / spontaneous) are split per block into their own epochs and carry no task metadata, listing the `Stimulus Screen` monitor as their active device. An `Optotagging` epoch (473 nm laser) is appended for ecephys sessions. A typical ecephys session is **7 epochs** (active, passive replay, flash, gabor, 2× spontaneous, optotagging); a behavior-only session is **1** (the active task).

**Ethics (IACUC) review id.** `acquisition.json`'s `ethics_review_id` is looked up per subject (6-digit mouse id) from `code/reference/ethics_review_ids.csv`, which covers **65 of the 81** Visual Behavior Neuropixels subjects. For the **16 uncovered subjects below**, the lookup warns and leaves `ethics_review_id` **None** rather than failing the session — so **all sessions of these 16 subjects (721 behavior-only + 32 ecephys = 753 sessions)** have no ethics review id. Extend `ethics_review_ids.csv` when the ids become available.

| subject (mouse id) | behavior sessions | ecephys sessions | ecephys session ids |
|---|---|---|---|
| 562033 | 91 | 2 | 1113751921, 1113957627 |
| 570299 | 54 | 2 | 1115077618, 1115356973 |
| 570302 | 77 | 2 | 1122903357, 1123100019 |
| 572846 | 31 | 2 | 1112302803, 1112515874 |
| 574078 | 30 | 2 | 1115086689, 1115368723 |
| 574081 | 51 | 2 | 1121406444, 1121607504 |
| 574082 | 40 | 2 | 1118327332, 1118508667 |
| 576323 | 23 | 2 | 1116941914, 1117148442 |
| 576324 | 28 | 2 | 1118324999, 1118512505 |
| 577287 | 48 | 2 | 1124285719, 1124507277 |
| 578003 | 27 | 2 | 1119946360, 1120251466 |
| 578257 | 52 | 2 | 1125713722, 1125937457 |
| 579993 | 53 | 2 | 1130113579, 1130349290 |
| 585326 | 24 | 2 | 1128520325, 1128719842 |
| 585329 | 57 | 2 | 1139846596, 1140102579 |
| 599294 | 35 | 2 | 1152632711, 1152811536 |

**Experimenters.** The acquisition's `experimenters` is left empty: the Visual Behavior Neuropixels session tables carry no per-session operator, and there is no operator reference file for this dataset (unlike Visual Coding Neuropixels). The project investigators are still recorded on the data description (Corbett Bennett, Shawn Olsen).

- Look into discrepancies in the session start time for some of the later sessions.