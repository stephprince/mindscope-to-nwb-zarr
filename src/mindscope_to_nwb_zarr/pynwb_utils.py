import numpy as np

from datetime import timedelta
from pynwb.base import TimeSeries
from pynwb.ecephys import ElectricalSeries
from hdmf.common import DynamicTable

from aind_data_schema_models.modalities import Modality

def get_latest_time(nwbfile):
    """Calculate latest time from NWB file by finding the latest timestamp across all TimeSeries"""
    max_time = None

    # get last timestamp across all TimeSeries
    for obj in nwbfile.all_children():
        if isinstance(obj, TimeSeries):
            if obj.timestamps is not None and len(obj.timestamps) > 0:
                last_time = obj.timestamps[-1]
            elif obj.starting_time is not None and obj.rate is not None:
                last_time = obj.starting_time + (obj.data.shape[0] / obj.rate)
            else:
                continue

            if max_time is None or last_time > max_time:
                max_time = last_time

        # Handle DynamicTable objects with time columns
        elif isinstance(obj, DynamicTable):
            if "stop_time" in obj.colnames and len(obj["stop_time"]):
                last_time = float(obj["stop_time"][-1])
            elif "spike_times" in obj.colnames and len(obj["spike_times"]):
                last_time = max(np.asarray(obj["spike_times"].target.data[:]) )

            if max_time is None or last_time > max_time:
                max_time = last_time
            
    return max_time

def get_acquisition_end_time(nwbfile):
    """Calculate acquisition end time from NWB file by finding the latest timestamp across all TimeSeries"""
    latest_time = get_latest_time(nwbfile)

    # Calculate end time
    if latest_time is not None:
        end_time = nwbfile.session_start_time + timedelta(seconds=float(latest_time))
    else:
        end_time = None

    return end_time

def get_modalities(nwbfile):
    modalities = set()
    # determine if ecephys modality present
    if len(nwbfile.units) > 0:
        modalities.add(Modality.ECEPHYS)

    electrical_series_types = [c for c in nwbfile.all_children() if isinstance(c, ElectricalSeries)]
    if len(electrical_series_types) > 0:
        modalities.add(Modality.ECEPHYS)

    # determine if behavior modality present (is this the best way to check?)
    if len(nwbfile.trials) > 0:
        modalities.add(Modality.BEHAVIOR)
    
    return list(modalities)