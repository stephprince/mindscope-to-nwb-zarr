---
jupytext:
  formats: md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.11.5
kernelspec:
  display_name: Python 3
  language: python
  name: swdb
---
```{code-cell} ipython3
import numpy as np
import matplotlib.pyplot as plt
%matplotlib inline
```
```{code-cell} ipython3
# from allensdk.core.brain_observatory_cache import BrainObservatoryCache
# manifest_file = '../../../data/allen-brain-observatory/visual-coding-2p/manifest.json'
# boc = BrainObservatoryCache(manifest_file=manifest_file)
```

```{code-cell} ipython3
from hdmf_zarr import NWBZarrIO

# NWB file paths for each session type
# TODO: Update these paths to the published Zarr file locations
nwb_paths = {
    'StimA': 'C:/Users/Ryan/results/visual-coding-ophys/sub-222426_ses-501704220-StimA_behavior+image+ophys.nwb.zarr',
    'StimB': 'C:/Users/Ryan/results/visual-coding-ophys/sub-222426_ses-501559087-StimB_behavior+image+ophys.nwb.zarr',
    'StimC': 'C:/Users/Ryan/results/visual-coding-ophys/sub-222426_ses-501474098-StimC_behavior+image+ophys.nwb.zarr',
}

# Mapping from stimulus name to session type
stimulus_to_session = {
    'drifting_gratings': 'StimA',
    'natural_movie_one': 'StimB',  # Also in StimA and StimC
    'natural_movie_three': 'StimA',
    'spontaneous': 'StimC',  # Also in StimA and StimB
    'static_gratings': 'StimB',
    'natural_scenes': 'StimB',
    'natural_movie_two': 'StimB',
    'locally_sparse_noise': 'StimC',
}

# Load all sessions and store in a dictionary
nwb_files = {}
for session_type, path in nwb_paths.items():
    io = NWBZarrIO(path, 'r')
    nwb_files[session_type] = io.read()

def get_nwb_for_stimulus(stimulus_name):
    """Return the NWB file containing the specified stimulus."""
    session_type = stimulus_to_session[stimulus_name]
    return nwb_files[session_type]

def get_dff_timestamps_for_stimulus(stimulus_name):
    """Return the DfOverF timestamps for the session containing the specified stimulus."""
    nwb = get_nwb_for_stimulus(stimulus_name)
    return nwb.processing["ophys"]["DfOverF"]["DfOverF"].timestamps[:]
```

# Getting data from a session

We're going to examine the data available for a single session. We can use the function `get_nwb_for_stimulus` defined above to get the NWB file for a specific stimulus. The NWB file contains all the data for this session, including neural activity, behavioral data, and stimulus information.

```{code-cell} ipython3
# experiment_container_id = 511510736
# session_id = boc.get_ophys_experiments(experiment_container_ids=[experiment_container_id], stimuli=['natural_scenes'])[0]['id']
```

```{code-cell} ipython3
# data_set = boc.get_ophys_experiment_data(ophys_experiment_id=session_id)
```

```{code-cell} ipython3
# Load the NWB file for the session containing natural scenes stimulus
nwb = get_nwb_for_stimulus('natural_scenes')
```

This `nwb` object gives us access to a lot of data! Let's explore:

(maximum_projection)=
## Maximum projection
This is the projection of the full motion corrected movie. It shows all of the cells imaged during the session.

```{code-cell} ipython3
# Summary images are stored in the ophys processing module
summary_images = nwb.processing["ophys"]["SummaryImages"]
max_projection = summary_images.images["maximum_intensity_projection"].data[:]
```

```{code-cell} ipython3
fig = plt.figure(figsize=(6,6))
plt.imshow(max_projection, cmap='gray')
plt.axis('off')
```

(roi_mask)=
## ROI Masks
{term}`ROI`s are all of the segmented masks for cell bodies identified in this session.

```{code-cell} ipython3
# ROI masks are stored in the PlaneSegmentation table
plane_seg = nwb.processing["ophys"]["ImageSegmentation"]["PlaneSegmentation"]

# Get pixel masks - stored as sparse format (list of [x, y, weight] for each ROI)
# We need to convert to dense image masks for visualization
n_rois = len(plane_seg.id[:])

# Get image dimensions from the imaging plane
image_height = plane_seg.imaging_plane.grid_spacing_unit  # This won't work, need actual dims
# The image dimensions are typically 512x512 for this dataset
image_height, image_width = 512, 512

def pixel_mask_to_image_mask(pixel_mask, height, width):
    """Convert sparse pixel_mask to dense image mask."""
    mask = np.zeros((height, width), dtype=np.float32)
    for x, y, weight in pixel_mask:
        mask[int(y), int(x)] = weight
    return mask

rois = np.array([pixel_mask_to_image_mask(plane_seg["pixel_mask"][i], image_height, image_width)
                 for i in range(n_rois)])
```

What is the shape of this array? How many neurons are in this experiment?

```{code-cell} ipython3
rois.shape
```

The first dimension of this array is the number of neurons, and each element of this axis is the mask of an individual neuron

Plot the masks for all the ROIs.

```{code-cell} ipython3
fig = plt.figure(figsize=(6,6))
plt.imshow(rois.sum(axis=0))
plt.axis('off');
```

## Fluorescence and DF/F traces
The NWB file contains a number of traces reflecting the processing that is done to the extracted fluorescence before we analyze it. The fluorescence traces are the mean fluorescence of all the pixels contained within a ROI mask.

There are a number of activity traces accessible in the NWB file, including raw fluorescence, neuropil corrected traces, demixed traces, and DF/F traces.

```{code-cell} ipython3
# Demixed fluorescence traces (spatially demixed traces of potentially overlapping masks)
demixed = nwb.processing["ophys"]["Fluorescence"]["Demixed"]
fluor = demixed.data[:].T  # Transpose to get (n_cells, n_timepoints)
timestamps = demixed.timestamps[:]
```

To correct from contamination from the neuropil, we perform neuropil correction. First, we extract a local neuropil signal, by creating a neuropil mask that is an annulus around the ROI mask (without containing any pixels from nearby neurons). You can see these neuropil signals in the neuropil traces:

```{code-cell} ipython3
# Neuropil fluorescence traces
neuropil = nwb.processing["ophys"]["Fluorescence"]["Neuropil"]
np_traces = neuropil.data[:].T  # Transpose to get (n_cells, n_timepoints)

# Get contamination ratios (r values) from the ContaminationRatios table
contamination = nwb.processing["ophys"]["ContaminationRatios"]
r_values = contamination["r"][:] if "r" in contamination.colnames else None
```

This neuropil trace is subtracted from the fluorescence trace, after being weighted by a factor ("r value") that is computed for each neuron. The resulting corrected fluorescence trace is accessed here along with the r_values.

```{code-cell} ipython3
# Corrected fluorescence traces (after neuropil subtraction)
corrected = nwb.processing["ophys"]["Fluorescence"]["Corrected"]
cor = corrected.data[:].T  # Transpose to get (n_cells, n_timepoints)
```

Let's look at these traces for one cell:

```{code-cell} ipython3
fig = plt.figure(figsize=(8,3))
plt.plot(timestamps, fluor[122,:], label='demixed')
plt.plot(timestamps, np_traces[122,:], label='neuropil')
plt.plot(timestamps, cor[122,:], label='corrected')
plt.xlabel("Time (s)")
plt.xlim(1900,2200)
plt.legend()
```

The signal we are most interested in the the DF/F - the change in fluorescence normalized by the baseline fluorescence. The baseline fluorescence was computed as the median fluorescence in a 180s window centered on each time point. The result is the dff trace:

```{code-cell} ipython3
# Get DfOverF traces in a RoiResponseSeries
dff_series = nwb.processing["ophys"]["DfOverF"]["DfOverF"]
dff = dff_series.data[:].T  # Transpose to get (n_cells, n_timepoints)
ts = dff_series.timestamps[:]

fig = plt.figure(figsize=(8,3))
plt.plot(ts, dff[122,:], color='gray')
plt.xlabel("Time (s)")
plt.xlim(1900,2200)
plt.ylabel("DFF")
```

(extracted_events)=
## Extracted events
We can also access events extracted from the DF/F traces using the L0 method developed by Sean Jewell and Daniella Witten.

```{code-cell} ipython3
# Get DfOverF events in a RoiResponseSeries
dff_events_series = nwb.processing["ophys"]["DfOverF"]["DfOverFEvents"]
dff_events = dff_events_series.data[:].T  # Transpose to get (n_cells, n_timepoints)
ts = dff_events_series.timestamps[:]
```


```{code-cell} ipython3
fig = plt.figure(figsize=(8,3))
plt.plot(ts, dff[122,:], color='gray')
plt.plot(ts, 2*dff_events[122,:]+5, color='black')
plt.xlabel("Time (s)")
plt.xlim(1900,2200)
plt.ylabel("DFF")
```

## Stimulus epochs
Several stimuli are shown during each imaging session, interleaved with each other. The stimulus epoch table provides information of these interleaved stimulus epochs, revealing when each epoch starts and ends. The start and end here are provided in terms of the imaging frame of the two-photon imaging. This allows us to index directly into the dff or event traces.

```{code-cell} ipython3
import pandas as pd
from pynwb import TimeIntervals

# Build stimulus epoch table from the stimulus intervals
stim_epochs = []
for stim_name in nwb.stimulus.keys():
    stim = nwb.stimulus[stim_name]
    if isinstance(stim, TimeIntervals):
        # TimeIntervals (like drifting_gratings, spontaneous)
        df = stim.to_dataframe()
        start_time = df['start_time'].min()
        stop_time = df['stop_time'].max()
    else:
        # IndexSeries (like natural_movie_one_stimulus)
        start_time = stim.timestamps[0]
        stop_time = stim.timestamps[-1]

    stim_epochs.append({
        'stimulus': stim_name.replace('_stimulus', ''),
        'start_time': start_time,
        'stop_time': stop_time
    })

stim_epoch = pd.DataFrame(stim_epochs).sort_values('start_time').reset_index(drop=True)

# Add start and end frame indices using the DfOverF timestamps
stim_epoch['start'] = np.searchsorted(ts, stim_epoch['start_time'])
stim_epoch['end'] = np.searchsorted(ts, stim_epoch['stop_time'])

stim_epoch
```

stimulus
: The name of the stimulus during the epoch

start
: The 2p imaging frame during which the epoch starts. This indexes directly into the activity traces (e.g. dff or extracted events) and behavior traces (e.g. running speed).

end
: The 2p imaging frame during which the epoch ends. This indexes directly into the activity traces (e.g. dff or extracted events) and behavior traces (e.g. running speed).

Let's plot the DFF traces of a number of cells and overlay stimulus epochs.

```{code-cell} ipython3
fig = plt.figure(figsize=(14,8))

#here we plot the first 50 neurons in the session
for i in range(50):
    plt.plot(dff[i,:]+(i*2), color='gray')

#here we shade the plot when each stimulus is presented
colors = ['blue','orange','green','red']
for c, stim_name in enumerate(stim_epoch.stimulus.unique()):
    stim = stim_epoch[stim_epoch.stimulus==stim_name]
    for j in range(len(stim)):
        plt.axvspan(xmin=stim.start.iloc[j], xmax=stim.end.iloc[j], color=colors[c], alpha=0.1)
```

(running_speed)=
## Running speed
The running speed of the animal on the rotating disk during the entire session. This has been temporally aligned to the two photon imaging, which means that this trace has the same length as dff (etc). This also means that the same stimulus start and end information indexes directly into this running speed trace.

```{code-cell} ipython3
# Running speed is stored in the behavior processing module
running_speed_series = nwb.processing["behavior"]["BehavioralTimeSeries"]['running_speed']
dxcm = running_speed_series.data[:]
running_timestamps = running_speed_series.timestamps[:]

print("length of dff: ", str(dff.shape[1]))
print("length of running speed: ", str(len(dxcm)))
```

Plot the running speed.

```{code-cell} ipython3
plt.plot(dxcm)
plt.ylabel("Running speed (cm/s)", fontsize=18)
```

Add the running speed to the neural activity and stimulus epoch figure

```{code-cell} ipython3
fig = plt.figure(figsize=(14,10))
for i in range(50):
    plt.plot(dff[i,:]+(i*2), color='gray')
plt.plot((0.2*dxcm)-20)

#for each stimulus, shade the plot when the stimulus is presented
colors = ['blue','orange','green','red']
for c, stim_name in enumerate(stim_epoch.stimulus.unique()):
    stim = stim_epoch[stim_epoch.stimulus==stim_name]
    for j in range(len(stim)):
        plt.axvspan(xmin=stim.start.iloc[j], xmax=stim.end.iloc[j], color=colors[c], alpha=0.1)
```

## Stimulus Table and Template
Each stimulus that is shown has a <b>stimulus table</b> that details what each trial is and when it is presented. Additionally, the <b>natural scenes</b>, <b>natural movies</b>, and <b>locally sparse noise</b> stimuli have a <b>stimulus template</b> that shows the exact image that is presented to the mouse. We detail how to access and use these items in [Visual stimuli](vc2p-stimuli.md).

(cell_ids_indices)=
## Cell ids and indices
Each neuron in the dataset has a unique id, called the <b>cell specimen id</b>. To find the neurons in this session, get the cell specimen ids from the PlaneSegmentation table.

```{code-cell} ipython3
# Cell IDs are stored in the PlaneSegmentation table
plane_seg = nwb.processing["ophys"]["ImageSegmentation"]["PlaneSegmentation"]
cell_ids = plane_seg.id[:]
cell_ids
```

Within each individual session, a cell id is associated with an index. This index maps into the dff traces.

```{code-cell} ipython3
# Find the index for a specific cell ID
target_cell_id = cell_ids[0]  # Use first cell as example
cell_index = np.where(cell_ids == target_cell_id)[0]
print(f"Cell ID {target_cell_id} is at index {cell_index}")
```

```{note}
As neurons are often matched across sessions, that neuron will have the same cell specimen id in all said sessions, but it will have a different cell specimen index in each session. This is explored in [Cross session data](vc2p-cross-session-data.md).
```

## Session metadata
Each file contains some metadata about that session including fields such as the mouse genotype, sex, and age, the session type, the targeted structure and imaging depth, when the data was acquired, and information about the instrument used to collect the data.

```{code-cell} ipython3
metadata = {
    'session_id': nwb.identifier,
    'session_start_time': nwb.session_start_time,
    'session_description': nwb.session_description,
    'subject_id': nwb.subject.subject_id,
    'genotype': nwb.subject.genotype,
    'sex': nwb.subject.sex,
    'age': nwb.subject.age,
    'species': nwb.subject.species,
    'institution': nwb.institution,
    'experiment_description': nwb.experiment_description,
}
metadata
```

