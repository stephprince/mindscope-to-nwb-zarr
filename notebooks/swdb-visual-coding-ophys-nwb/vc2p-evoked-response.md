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
import pandas as pd
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

# Load the StimB session which contains natural_scenes
io = NWBZarrIO(nwb_paths['StimB'], 'r')
nwb = io.read()
```

# Exercise: Evoked response

We want to put these different pieces of data together. Here will will find the preferred image for a neuron - the image among the natural scenes that drives the largest mean response - and see how that response is modulated by the mouse's running activity.

```{code-cell} ipython3
# cell_specimen_id = 517474020
# session_id = boc.get_ophys_experiments(cell_specimen_ids=[cell_specimen_id], stimuli=['natural_scenes'])[0]['id']
# data_set = boc.get_ophys_experiment_data(ophys_experiment_id=session_id)
```

Let's get a cell index for this exercise. We'll use the first cell in the session:

```{code-cell} ipython3
# Get cell IDs from the PlaneSegmentation table
plane_seg = nwb.processing["ophys"]["ImageSegmentation"]["PlaneSegmentation"]
cell_ids = plane_seg.id[:]

# Use the first cell for this exercise
cell_index = 0
cell_specimen_id = cell_ids[cell_index]
print("Cell specimen ID: ", str(cell_specimen_id))
print("Cell index: ", str(cell_index))
```

Let's start with the DF/F traces:

```{code-cell} ipython3
# Get DfOverF traces
dff_series = nwb.processing["ophys"]["DfOverF"]["DfOverF"]
dff = dff_series.data[:].T  # Transpose to get (n_cells, n_timepoints)
ts = dff_series.timestamps[:]

plt.plot(ts, dff[cell_index,:])
plt.xlabel("Time (s)")
plt.ylabel("DF/F (%)")
```

Let's get the stimulus table for the natural scenes:

```{code-cell} ipython3
# Natural scenes stimulus information is stored as an NWB IndexSeries
natural_scenes = nwb.stimulus["natural_scenes_stimulus"]

# Create a DataFrame similar to the AllenSDK format
stim_table = pd.DataFrame({
    'frame': natural_scenes.data[:],
})

# Add start and end frame indices using the DfOverF timestamps
stim_table['start'] = np.searchsorted(ts, natural_scenes.timestamps[:])
# Compute end times from consecutive start times, then convert to frame indices
# End is inclusive (last frame of trial), matching AllenSDK convention
stop_times = np.append(natural_scenes.timestamps[1:], ts[-1])
stim_table['end'] = np.searchsorted(ts, stop_times) - 1

stim_table.head(n=10)
```

## Which image is the preferred image for this neuron?

For each trial of the natural scenes stimulus let's compute the mean response of the neuron's response and the mean running speed of the mouse.

```{code-cell} ipython3
num_trials = len(stim_table)

trial_response = np.empty((num_trials))

for index, row in stim_table.iterrows():
    trial_response[index] = dff[cell_index, row.start:row.start+14].mean()

image_response = np.empty((119)) #number of images + blanksweep
image_sem = np.empty((119))
for i in range(-1,118):
    trials = stim_table[stim_table.frame==i].index.values
    image_response[i+1] = trial_response[trials].mean()
    image_sem[i+1] = trial_response[trials].std()/np.sqrt(len(trials))

plt.errorbar(range(-1,118), image_response, yerr=image_sem, fmt='o')
plt.xlabel("Image number")
plt.ylabel("Mean DF/F (%)")
```

Which image is the preferred image?

```{code-cell} ipython3
preferred_image = np.argmax(image_response) - 1
print(preferred_image)

# Get the natural scenes template from the IndexSeries
natural_scene_template = natural_scenes.indexed_images
image = natural_scene_template.order_of_images[preferred_image]
plt.imshow(image.data[:], cmap='gray')
plt.axis('off');
```

## How does the running activity influence the neuron's response to this image?

Let's get the running speed of the mouse:

```{code-cell} ipython3
# Running speed is stored in the behavior processing module
running_speed_series = nwb.processing["behavior"]["BehavioralTimeSeries"]['running_speed']
dxcm = running_speed_series.data[:]

plt.plot(ts, dxcm)
plt.xlabel("Time (s)")
plt.ylabel("Running speed (cm/s)")
```

Compute the mean running speed during each trial. We will make a pandas dataframe with the mean trial response and the mean running speed

```{code-cell} ipython3
trial_speed = np.empty((num_trials))

for index, row in stim_table.iterrows():
    trial_speed[index] = dxcm[row.start:row.end].mean()

df = pd.DataFrame(columns=('response','speed'), index=stim_table.index)
df.response = trial_response
df.speed = trial_speed

stationary_mean = df[(stim_table.frame==preferred_image)&(df.speed<2)].mean()
running_mean = df[(stim_table.frame==preferred_image)&(df.speed>2)].mean()
print("Stationary mean response = ", np.round(stationary_mean.response.mean(),4))
print("Running mean response = ", np.round(running_mean.response.mean(),4))
```

Let's plot the trial responses when the mouse is stationary (blue) with those when the mouse is running (red).

```{code-cell} ipython3
running_trials = stim_table[(stim_table.frame==preferred_image)&(df.speed>2)].index.values
stationary_trials = stim_table[(stim_table.frame==preferred_image)&(df.speed<2)].index.values

for trial in stationary_trials:
    plt.plot(dff[cell_index, stim_table.start.loc[trial]-30:stim_table.end.loc[trial]+30], color='blue', alpha=0.5)
for trial in running_trials:
    plt.plot(dff[cell_index, stim_table.start.loc[trial]-30:stim_table.end.loc[trial]+30], color='red', alpha=0.5)
plt.axvspan(xmin=30, xmax=37, color='gray', alpha=0.1)
plt.xlabel("Time (frames)")
plt.ylabel("DF/F (%)")
```

There is a lot of trial-to-trial variability in the response of this neuron to its preferred image. On average, the response during running is larger than that when the mouse is stationary, but there is considerable overlap in the two distributions.
