import numpy as np

from datetime import datetime, timedelta
from pynwb import NWBFile
from pynwb.base import TimeSeries
from pynwb.ecephys import ElectricalSeries
from hdmf.common import DynamicTable

from aind_data_schema_models.modalities import Modality

def get_latest_time(nwbfile: NWBFile) -> float | None:
    """Calculate latest time from NWB file by finding the latest timestamp across all TimeSeries"""
    max_time = None

    for obj in nwbfile.all_children():
         # get last timestamp across all TimeSeries
        if isinstance(obj, TimeSeries):
            if obj.timestamps is not None and len(obj.timestamps) > 0:
                last_time = obj.timestamps[-1]
            elif obj.starting_time is not None and obj.rate is not None:
                last_time = obj.starting_time + (obj.data.shape[0] / obj.rate)

        # Handle DynamicTable objects with time columns
        elif isinstance(obj, DynamicTable):
            if "stop_time" in obj.colnames and len(obj["stop_time"]):
                last_time = float(obj["stop_time"][-1])
            elif "spike_times" in obj.colnames and len(obj["spike_times"]):
                last_time = max(np.asarray(obj["spike_times"].target.data[:]))
            else:
                continue
        else:
            continue

        if max_time is None or last_time > max_time:
            max_time = last_time
            
    return max_time

def get_data_stream_end_time(nwbfile: NWBFile) -> datetime | None:
    """Calculate acquisition end time from NWB file by finding the latest timestamp across all TimeSeries"""
    latest_time = get_latest_time(nwbfile)

    # Calculate end time
    if latest_time is not None:
        end_time = nwbfile.session_start_time + timedelta(seconds=float(latest_time))
    else:
        end_time = None

    return end_time

def get_data_stream_start_time(nwbfile: NWBFile) -> datetime | None:
    earliest_time = get_earliest_time(nwbfile)

    # Calculate end time
    if earliest_time is not None:
        start_time = nwbfile.session_start_time + timedelta(seconds=float(earliest_time))
    else:
        start_time = None

    return start_time

def get_modalities(nwbfile: NWBFile) -> list[Modality]:
    modalities = set()
    # determine if ecephys modality present
    if nwbfile.units and len(nwbfile.units) > 0:
        modalities.add(Modality.ECEPHYS)

    electrical_series_types = [c for c in nwbfile.all_children() if isinstance(c, ElectricalSeries)]
    if len(electrical_series_types) > 0:
        modalities.add(Modality.ECEPHYS)

    # determine if behavior modality present (is this the best way to check?)
    if nwbfile.trials and len(nwbfile.trials) > 0:
        modalities.add(Modality.BEHAVIOR)

    if nwbfile.imaging_planes and len(nwbfile.imaging_planes) > 0:
        modalities.add(Modality.POPHYS)
    
    return list(modalities)


def get_earliest_time(nwbfile: NWBFile) -> datetime | None:
    """Calculate data stream start time from NWB file by finding the earliest timestamp across all TimeSeries"""
    earliest_time = None

    # get last timestamp across all TimeSeries
    for obj in nwbfile.all_children():
        if isinstance(obj, TimeSeries):
            if obj.timestamps is not None and len(obj.timestamps) > 0:
                start_time = obj.timestamps[0]
            elif obj.starting_time is not None and obj.rate is not None:
                start_time = obj.starting_time
            
        elif isinstance(obj, DynamicTable):
            if "start_time" in obj.colnames and len(obj["start_time"]):
                start_time = float(obj["start_time"][0])
            elif "spike_times" in obj.colnames and len(obj["spike_times"]):
                start_time = min(np.asarray(obj["spike_times"].target.data[:]) )
            else:
                continue
        else:
            continue

        if earliest_time is None or start_time < earliest_time:
            earliest_time = start_time
    
    return earliest_time
