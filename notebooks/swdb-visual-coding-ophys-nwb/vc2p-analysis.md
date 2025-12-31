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
import scipy.stats as st
%matplotlib inline
```

```{code-cell} ipython3
from hdmf_zarr import NWBZarrIO

# NWB file paths for each session type
# TODO: Update these paths to the published Zarr file locations
nwb_paths = {
    'StimA': 'C:/Users/Ryan/results/sub-222426_ses-501704220-StimA_behavior+image+ophys.nwb',
    'StimB': 'C:/Users/Ryan/results/sub-222426_ses-501559087-StimB_behavior+image+ophys.nwb',
    'StimC': 'C:/Users/Ryan/results/sub-222426_ses-501474098-StimC_behavior+image+ophys.nwb',
}

# Load the StimB session which contains natural_scenes
io = NWBZarrIO(nwb_paths['StimB'], 'r')
nwb = io.read()
```

```{code-cell} ipython3
# from allensdk.core.brain_observatory_cache import BrainObservatoryCache
# manifest_file = '../../../data/allen-brain-observatory/visual-coding-2p/manifest.json'
# boc = BrainObservatoryCache(manifest_file=manifest_file)
# experiment_container_id = 511510736
# session_id = boc.get_ophys_experiments(experiment_container_ids=[experiment_container_id], stimuli=['natural_scenes'])[0]['id']
# data_set = boc.get_ophys_experiment_data(ophys_experiment_id=session_id)
```

# Analysis examples and cell specimens table

## Analysis examples

```{code-cell} ipython3
# from allensdk.brain_observatory.natural_scenes import NaturalScenes
# ns = NaturalScenes(data_set)

# Get the required data from the NWB file:

# Get corrected fluorescence traces (used for sweep_response computation)
fluor_series = nwb.processing["ophys"]["Fluorescence"]["DemixedTraces"]
timestamps = fluor_series.timestamps[:]
celltraces = fluor_series.data[:].T  # Shape: (n_cells, n_timepoints)
numbercells = celltraces.shape[0]
acquisition_rate = 1 / (timestamps[1] - timestamps[0])

# Get DfOverF traces
dff_series = nwb.processing["ophys"]["DfOverF"]["DfOverF"]
dfftraces = dff_series.data[:].T  # Shape: (n_cells, n_timepoints)

# Get running speed
running_speed_series = nwb.processing["behavior"]["BehavioralTimeSeries"]['running_speed']
dxcm = running_speed_series.data[:]

print(f"Number of cells: {numbercells}")
print(f"Acquisition rate: {acquisition_rate:.2f} Hz")
print(f"Number of timepoints: {len(timestamps)}")
```

```{code-cell} ipython3
# Get the natural scenes stimulus table
natural_scenes = nwb.stimulus["natural_scenes_stimulus"]

stim_table = pd.DataFrame({
    'frame': natural_scenes.data[:],
})
stim_table['start'] = np.searchsorted(timestamps, natural_scenes.timestamps[:])
stim_table['end'] = np.append(stim_table['start'].values[1:], len(timestamps))

# Calculate stimulus timing parameters
sweeplength = stim_table.end.iloc[1] - stim_table.start.iloc[1]
interlength = 4 * sweeplength  # ~1 second before stimulus
extralength = sweeplength  # ~0.25 seconds after stimulus

number_scenes = len(np.unique(stim_table.frame))

print(f"Number of natural scene images: {number_scenes}")
print(f"Sweep length: {sweeplength} frames ({sweeplength/acquisition_rate:.2f} s)")
print(f"Inter length: {interlength} frames ({interlength/acquisition_rate:.2f} s)")
```

For each stimulus, we compute two dataframes called `sweep_response` and `mean_sweep_response` that quantify the individual trial responses of each neuron. The sweep_response dataframe contains the DF/F for each neuron for each trial. The index of the dataframe matches the stimulus table for the stimulus, and the columns are the cell indexes (as strings).

For this dataframe, DF/F was computed using the mean fluorescence in the 1 second prior to the start of the trial as the Fo. The sweep response contains this DF/F for each neuron spanning from 1 second before the start of the trial to 1 second after the end of the trial. In addition to the responses of each neuron, there is one additional column that captures the running speed of the mouse during the same time span of each trial. This column is titled 'dx'.

The mean_sweep_response (with the same index and columns as sweep_response) calculates the mean value of the DF/F in the sweep response dataframe during each trial for each neuron. The column titled 'dx' averages the running speed in the same way.

```{code-cell} ipython3
def get_sweep_response(stim_table, celltraces, dxcm, interlength, sweeplength, extralength):
    """
    Calculates the response to each sweep in the stimulus table for each cell.

    Returns:
        sweep_response: pd.DataFrame of response dF/F traces (column per cell, row per sweep)
        mean_sweep_response: mean values of the traces in sweep_response
        pval: p-value from 1-way ANOVA comparing response during sweep to response prior to sweep
    """
    numbercells = celltraces.shape[0]

    def do_mean(x):
        return np.mean(x[interlength:interlength + sweeplength + extralength])

    def do_p_value(x):
        (_, p) = st.f_oneway(
            x[:interlength],
            x[interlength:interlength + sweeplength + extralength])
        return p

    sweep_response = pd.DataFrame(
        index=stim_table.index.values,
        columns=list(map(str, range(numbercells + 1)))
    )
    sweep_response.rename(columns={str(numbercells): 'dx'}, inplace=True)

    for index, row in stim_table.iterrows():
        start = int(row['start'] - interlength)
        end = int(row['start'] + sweeplength + interlength)

        for nc in range(numbercells):
            temp = celltraces[nc, start:end]
            # Compute DF/F using baseline as mean of interlength before stimulus
            sweep_response[str(nc)][index] = 100 * ((temp / np.mean(temp[:interlength])) - 1)
        sweep_response['dx'][index] = dxcm[start:end]

    mean_sweep_response = sweep_response.applymap(do_mean)
    pval = sweep_response.applymap(do_p_value)

    return sweep_response, mean_sweep_response, pval

# Compute sweep responses
sweep_response, mean_sweep_response, pval = get_sweep_response(
    stim_table, celltraces, dxcm, interlength, sweeplength, extralength
)
```

```{code-cell} ipython3
plt.plot(sweep_response['0'].loc[0])
plt.axvline(x=30, ls='--', color='k')
plt.xlabel("Frames")
plt.ylabel("DF/F (%)")
plt.title("Response of cell index 0 to the first trial")
print("Mean response of cell index 0 to the first trial:", mean_sweep_response['0'].loc[0])
```

In addition to these dataframes we compute a numpy array named `response` that captures the mean response to each stimulus condition. For example, for the drifting grating stimulus, this array has the shape of (8,6,3,number_cells+1). The first dimension is the stimulus direction, the second dimension is the temporal frequency plus the blank sweep. The third dimension is [mean response, standard deviation of the response, number of trials of the condition that are significant]. And the last dimension is all the neurons plus the running speed in the last element. So the mean response of, say, cell index 17, to the blank sweep is located at response[0,0,0,17]. For natural scenes this has a shape of (119,3,number_cells+1).

```{code-cell} ipython3
def get_response(stim_table, mean_sweep_response, pval, number_scenes, numbercells):
    """
    Computes the mean response for each cell to each stimulus condition.

    Returns:
        response: (number_scenes, numbercells+1, 3) np.ndarray
            Final dimension contains: [mean response, SEM, number of significant trials]
    """
    response = np.empty((number_scenes, numbercells + 1, 3))

    def ptest(x):
        return len(np.where(x < (0.05 / (number_scenes - 1)))[0])

    for ns in range(number_scenes):
        subset_response = mean_sweep_response[stim_table.frame == (ns - 1)]
        subset_pval = pval[stim_table.frame == (ns - 1)]
        response[ns, :, 0] = subset_response.mean(axis=0)
        response[ns, :, 1] = subset_response.std(axis=0) / np.sqrt(len(subset_response))
        response[ns, :, 2] = subset_pval.apply(ptest, axis=0)

    return response

response = get_response(stim_table, mean_sweep_response, pval, number_scenes, numbercells)
print(f"Response array shape: {response.shape}")
print(f"Expected shape: ({number_scenes}, {numbercells + 1}, 3)")
```

Within this analysis object, there are useful functions to calculate signal and noise correlations called `get_signal_correlation` and `get_noise_correlation`. These return arrays of the signal and noise correlations of all the neurons in a session for this specific stimulus. The shape of the array is (number_cells, number_cells).

```{code-cell} ipython3
def get_signal_correlation(response, numbercells, corr='spearman'):
    """
    Calculate signal correlations between neurons based on their mean responses.

    Returns:
        signal_corr: (numbercells, numbercells) correlation matrix
        signal_p: (numbercells, numbercells) p-value matrix
    """
    resp = response[:, :, 0].T  # Shape: (numbercells+1, number_scenes)
    resp = resp[:numbercells, :]  # Exclude running speed column
    N, Nstim = resp.shape

    signal_corr = np.zeros((N, N))
    signal_p = np.empty((N, N))

    if corr == 'pearson':
        for i in range(N):
            for j in range(i, N):
                signal_corr[i, j], signal_p[i, j] = st.pearsonr(resp[i], resp[j])
    elif corr == 'spearman':
        for i in range(N):
            for j in range(i, N):
                signal_corr[i, j], signal_p[i, j] = st.spearmanr(resp[i], resp[j])
    else:
        raise ValueError('correlation should be pearson or spearman')

    # Fill in lower triangle (matrix is symmetric)
    signal_corr = np.triu(signal_corr) + np.triu(signal_corr, 1).T
    signal_p = np.triu(signal_p) + np.triu(signal_p, 1).T

    return signal_corr, signal_p

sc, sc_p = get_signal_correlation(response, numbercells)
plt.imshow(sc)
plt.colorbar(label='Correlation')
plt.xlabel("Cell index")
plt.ylabel("Cell index")
plt.title("Signal correlation")
```

## Cell specimen table
In addition to the analysis tables, we can compute response metrics for each neuron using the responses that are stored in the analysis files. These metrics describe the visual activity and response properties of the neurons and can be useful in identifying relevant neurons for analysis. Each metric name has a suffix that is the abbreviation of the stimulus it was computed from (e.g. dg=drifting gratings, lsn=locally sparse noise). These metrics and how they were computed are described extensively in this [whitepaper](https://community.brain-map.org/uploads/short-url/uOe7nlLdLLIIivh5PeL8a0g7gV7.pdf).

```{code-cell} ipython3
# cell_specimen_table = pd.DataFrame(boc.get_cell_specimens())
# print(cell_specimen_table.keys())
# cell_specimen_table.head()

# Basic cell information is available in the PlaneSegmentation table:
plane_seg = nwb.processing["ophys"]["ImageSegmentation"]["PlaneSegmentation"]
cell_ids = plane_seg.id[:]

cell_info = pd.DataFrame({
    'cell_specimen_id': cell_ids,
    'cell_index': range(len(cell_ids)),
})
print(f"Number of cells: {len(cell_info)}")
cell_info.head()
```

```{code-cell} ipython3
# Compute preferred natural scene for each cell
preferred_scene = np.argmax(response[1:, :numbercells, 0], axis=0)  # Exclude blank sweep
peak_dff = response[1:, :numbercells, 0].max(axis=0)

cell_info['scene_ns'] = preferred_scene
cell_info['peak_dff_ns'] = peak_dff
cell_info.head()
```

## Caveat
The analysis file and the metrics in the cell specimen table were computed from the DF/F as described above. While this is not incorrect, per se, there are some caveats to this. Metrics such as DSI which are defined as (pref-null)/(pref+null) are expected to be contained to +/- 1. However, we can have trials with negative DF/F, especially using it as we do here, in which case these metrics will not be contained in this way. This can make it difficult to interpret the resulting values.

These analysis objects and metrics are not invalid, but be sure to use and interpret them appropriately.
