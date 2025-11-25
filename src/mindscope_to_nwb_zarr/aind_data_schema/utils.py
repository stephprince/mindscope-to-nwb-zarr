
from datetime import datetime, timezone
import warnings

def get_subject_id(nwbfile, session_info=None):
    if session_info is not None:
        assert session_info['mouse_id'].values[0] == int(nwbfile.subject.subject_id), "subject_id mismatch occurred"
    return nwbfile.subject.subject_id

def get_session_start_time(nwbfile, session_info=None):
    if session_info is not None:
        session_time = datetime.fromisoformat(session_info['date_of_acquisition'].values[0])
        session_time_utc = session_time.astimezone(timezone.utc).replace(microsecond=0)
        nwb_time_utc = nwbfile.session_start_time.astimezone(timezone.utc).replace(microsecond=0)   

        if session_time_utc != nwb_time_utc:
            warnings.warn(
                f"session_start_time mismatch - using nwbfile value. "
                f"session_info={session_time_utc}, nwbfile={nwb_time_utc}"
            )

    return nwbfile.session_start_time

def get_instrument_id(nwbfile, session_info=None):
    instrument = next(iter(nwbfile.devices))
    if session_info is not None:
        assert session_info['equipment_name'].values[0] == instrument, "instrument_id mismatch occurred"
    return instrument

def get_total_reward_volume(nwbfile):
    if 'reward_volume' in nwbfile.trials.colnames:
        return float(nwbfile.trials['reward_volume'][:].sum())
    return None