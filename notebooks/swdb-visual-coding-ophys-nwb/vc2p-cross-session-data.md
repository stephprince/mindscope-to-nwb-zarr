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

# Load all sessions and store in a dictionary
nwb_files = {}
for session_type, path in nwb_paths.items():
    io = NWBZarrIO(path, 'r')
    nwb_files[session_type] = io.read()

nwb_a = nwb_files['StimA']
nwb_b = nwb_files['StimB']
nwb_c = nwb_files['StimC']
```

# Cross session data

As many neurons are imaged across multiple session, many analyses require accessing data for those neurons from multiple data files.

As mentioned in [Cell ids and indices](vc2p-session-data.md), when neurons are matched across multiple sessions they have the same <b>cell specimen id</b> but they have a different <b>cell index</b> within each session.

Let's consider one neuron

```{code-cell} ipython3
cell_specimen_id = 517472450
```

We can find the sessions for which this neuron was recorded using the `get_ophys_experiments()` and passing a list that contains this cell specimen id.

```{code-cell} ipython3
# pd.DataFrame(boc.get_ophys_experiments(cell_specimen_ids=[cell_specimen_id]))
```

As we expect, all of these sessions are from the same <b>experiment container id</b> each from a different <b>session type</b>. If this is not clear, refer to [Experiment containers and session](experiment_containers_sessions).

Let's explore this neuron across all three sessions. We already loaded the data for all three sessions above.

```{code-cell} ipython3
# id_a = boc.get_ophys_experiments(cell_specimen_ids=[cell_specimen_id], session_types=['three_session_A'])[0]['id']
# id_b = boc.get_ophys_experiments(cell_specimen_ids=[cell_specimen_id], session_types=['three_session_B'])[0]['id']
# id_c = boc.get_ophys_experiments(cell_specimen_ids=[cell_specimen_id], session_types=['three_session_C'])[0]['id']

# data_set_a = boc.get_ophys_experiment_data(id_a)
# data_set_b = boc.get_ophys_experiment_data(id_b)
# data_set_c = boc.get_ophys_experiment_data(id_c)
```

Let's find the cell index for this neuron in each of these sessions. Recall that there are different [cell indices](cell_ids_indices) for each session even thought the neuron has a single cell id.

```{code-cell} ipython3
# Get cell IDs from each session
cell_ids_a = nwb_a.processing["ophys"]["ImageSegmentation"]["PlaneSegmentation"].id[:]
cell_ids_b = nwb_b.processing["ophys"]["ImageSegmentation"]["PlaneSegmentation"].id[:]
cell_ids_c = nwb_c.processing["ophys"]["ImageSegmentation"]["PlaneSegmentation"].id[:]

# Find cell index in each session
index_a = np.where(cell_ids_a == cell_specimen_id)[0][0]
index_b = np.where(cell_ids_b == cell_specimen_id)[0][0]
index_c = np.where(cell_ids_c == cell_specimen_id)[0][0]

print("Session A index: ", str(index_a))
print("Session B index: ", str(index_b))
print("Session C index: ", str(index_c))
```

We can visualize the [maximum projection](maximum_projection) for each session and see the [ROI mask](roi_mask) of the neuron in each session.

```{code-cell} ipython3
# Get max projections from each session
mp_a = nwb_a.processing["ophys"]["SummaryImages"].images["max_projection"].data[:]
mp_b = nwb_b.processing["ophys"]["SummaryImages"].images["max_projection"].data[:]
mp_c = nwb_c.processing["ophys"]["SummaryImages"].images["max_projection"].data[:]

# Get ROI masks from each session
plane_seg_a = nwb_a.processing["ophys"]["ImageSegmentation"]["PlaneSegmentation"]
plane_seg_b = nwb_b.processing["ophys"]["ImageSegmentation"]["PlaneSegmentation"]
plane_seg_c = nwb_c.processing["ophys"]["ImageSegmentation"]["PlaneSegmentation"]

rois_a = np.array([plane_seg_a["image_mask"][i] for i in range(len(cell_ids_a))])
rois_b = np.array([plane_seg_b["image_mask"][i] for i in range(len(cell_ids_b))])
rois_c = np.array([plane_seg_c["image_mask"][i] for i in range(len(cell_ids_c))])

fig = plt.figure(figsize=(12,5))
ax1 = plt.subplot(131)
ax1.imshow(mp_a, cmap='gray')
ax1.axis('off')
ax2 = plt.subplot(132)
ax2.imshow(mp_b, cmap='gray')
ax2.axis('off')
ax3 = plt.subplot(133)
ax3.imshow(mp_c, cmap='gray')
ax3.axis('off')

fig2 = plt.figure(figsize=(12,5))
ax4 = plt.subplot(131)
ax4.imshow(rois_a[index_a,:,:])
ax4.axis('off')
ax5 = plt.subplot(132)
ax5.imshow(rois_b[index_b,:,:])
ax5.axis('off')
ax6 = plt.subplot(133)
ax6.imshow(rois_c[index_c,:,:])
ax6.axis('off')
```

Let's look at the activity of this neuron in each session. We'll use the DF/F.

```{code-cell} ipython3
# Get DfOverF traces from each session
dff_series_a = nwb_a.processing["ophys"]["DfOverF"]["DfOverF"]
dff_series_b = nwb_b.processing["ophys"]["DfOverF"]["DfOverF"]
dff_series_c = nwb_c.processing["ophys"]["DfOverF"]["DfOverF"]

dff_a = dff_series_a.data[:].T  # Transpose to get (n_cells, n_timepoints)
dff_b = dff_series_b.data[:].T
dff_c = dff_series_c.data[:].T

ts_a = dff_series_a.timestamps[:]
ts_b = dff_series_b.timestamps[:]
ts_c = dff_series_c.timestamps[:]

plt.figure(figsize=(12,8))
ax1 = plt.subplot(311)
ax1.plot(ts_a,dff_a[index_a,:])
ax1.set_xlabel("Time (s)")
ax1.set_ylabel("DF/F (%)")
ax2 = plt.subplot(312)
ax2.plot(ts_b,dff_b[index_b,:])
ax2.set_xlabel("Time (s)")
ax2.set_ylabel("DF/F (%)")
ax3 = plt.subplot(313)
ax3.plot(ts_c,dff_c[index_c,:])
ax3.set_xlabel("Time (s)")
ax3.set_ylabel("DF/F (%)")
```

Each experiment session consists of a different set of stimuli, but in each session has two things: at least one epoch of [spontaneous activity](spontaneous_activity) and [natural movie one](natural_movie).

Let's compute the mean response of this neuron's response to natural movie one in each session and compare them.

```{code-cell} ipython3
# Get natural movie one stimulus from each session
nm_a_stim = nwb_a.stimulus["natural_movie_one_stimulus"]
nm_b_stim = nwb_b.stimulus["natural_movie_one_stimulus"]
nm_c_stim = nwb_c.stimulus["natural_movie_one_stimulus"]

# Create stimulus tables with start frame indices
nm_stim_a = pd.DataFrame({'frame': nm_a_stim.data[:]})
nm_stim_a['start'] = np.searchsorted(ts_a, nm_a_stim.timestamps[:])

nm_stim_b = pd.DataFrame({'frame': nm_b_stim.data[:]})
nm_stim_b['start'] = np.searchsorted(ts_b, nm_b_stim.timestamps[:])

nm_stim_c = pd.DataFrame({'frame': nm_c_stim.data[:]})
nm_stim_c['start'] = np.searchsorted(ts_c, nm_c_stim.timestamps[:])

repeat_starts_a = nm_stim_a[nm_stim_a.frame==0].start.values
repeat_starts_b = nm_stim_b[nm_stim_b.frame==0].start.values
repeat_starts_c = nm_stim_c[nm_stim_c.frame==0].start.values

nm_a = np.empty((900,10))
nm_b = np.empty((900,10))
nm_c = np.empty((900,10))
for i in range(10):
  start_a = repeat_starts_a[i]
  nm_a[:,i] = dff_a[index_a,start_a:start_a+900]
  start_b = repeat_starts_b[i]
  nm_b[:,i] = dff_b[index_b,start_b:start_b+900]
  start_c = repeat_starts_c[i]
  nm_c[:,i] = dff_c[index_c,start_c:start_c+900]

plt.figure(figsize=(10,4))
plt.plot(nm_a.mean(axis=1), label="Session A")
plt.plot(nm_b.mean(axis=1), label="Session B")
plt.plot(nm_c.mean(axis=1), label="Session C")

plt.legend()
plt.xlabel("Time (imaging frames)")
plt.ylabel("DF/F (%)")
```

How similar are these mean responses?

```{code-cell} ipython3
import scipy.stats as st
r,p = st.pearsonr(nm_a.mean(axis=1), nm_b.mean(axis=1))
print("AB Correlation = ", str(r))

r,p = st.pearsonr(nm_a.mean(axis=1), nm_c.mean(axis=1))
print("AC Correlation = ", str(r))

r,p = st.pearsonr(nm_b.mean(axis=1), nm_c.mean(axis=1))
print("BC Correlation = ", str(r))
```

