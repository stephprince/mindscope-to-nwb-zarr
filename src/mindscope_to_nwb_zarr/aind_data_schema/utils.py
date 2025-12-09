
import json
import numpy as np
import pandas as pd
import warnings

from datetime import datetime, timezone
from pynwb import NWBFile


def get_subject_id(nwbfile: NWBFile, session_info: pd.DataFrame) -> str:
    """Get the subject ID from the NWB file, cross-checked with the session info. e.g., "457841"."""
    assert session_info['mouse_id'].values[0] == int(nwbfile.subject.subject_id), "subject_id mismatch occurred"
    return nwbfile.subject.subject_id

def get_session_start_time(nwbfile: NWBFile, session_info: pd.DataFrame) -> datetime:
    """Get the session start time from the NWB file, cross-checked with the session info. 
    e.g., datetime object for 2018-08-24T14:51:25.667000+00:00
    """
    session_time = datetime.fromisoformat(session_info['date_of_acquisition'].values[0])
    session_time_utc = session_time.astimezone(timezone.utc).replace(microsecond=0)
    nwb_time_utc = nwbfile.session_start_time.astimezone(timezone.utc).replace(microsecond=0)   

    if session_time_utc != nwb_time_utc:
        warnings.warn(
            f"session_start_time mismatch - using nwbfile value. "
            f"session_info={session_time_utc}, nwbfile={nwb_time_utc}"
        )

    return nwbfile.session_start_time

def get_instrument_id(nwbfile: NWBFile, session_info) -> str:
    """Get the instrument ID from the NWB file, cross-checked with the session info. e.g. "BEH.F-Box1"."""
    instrument = next(iter(nwbfile.devices))
    assert session_info['equipment_name'].values[0] == instrument, "instrument_id mismatch occurred"
    return instrument

def get_total_reward_volume(nwbfile: NWBFile) -> float | None:
    if 'reward_volume' in nwbfile.trials.colnames:
        return float(nwbfile.trials['reward_volume'][:].sum())
    return None

def get_individual_reward_volume(nwbfile: NWBFile) -> float | None:
    if 'reward_volume' in nwbfile.trials.colnames:
        volumes = nwbfile.intervals['trials'].to_dataframe()['reward_volume'].unique()
        volumes = volumes[volumes > 0]
        if len(volumes) > 1:
            warnings.warn(f"Multiple non-zero reward volumes found: {volumes}. Using the first one.")
        return float(volumes[0])
    
    return None

def get_curriculum_status(session_info):
    # NOTE - nwbfile.lab_meta_data['task_parameters'] also has several task parameters for behavior files that might be useful to record
    keys = ["experience_level", "image_set", "session_number", "prior_exposures_to_image_set",
            "prior_exposures_to_omissions", "prior_exposures_to_session_type"]
    curriculum_dict = {k: session_info[k].values[0] for k in keys if k in session_info.columns}
    
    return json.dumps(curriculum_dict, cls=NumpyJsonEncoder)

def serialized_dict(**kwargs) -> str:
    return json.dumps(dict(**kwargs), cls=NumpyJsonEncoder)


class NumpyJsonEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle NumPy data types."""
    def default(self, obj):
        if isinstance(obj, (np.integer, np.floating, np.ndarray)):
            return obj.tolist()
        return super().default(obj)