
import json
import re
import numpy as np
import pandas as pd
import warnings

from aind_data_schema_models.brain_atlas import CCFv3
from aind_data_schema.components.stimulus import VisualStimulation, PulseShape
from aind_data_schema.components.configs import ProbeConfig
from aind_data_schema.components.coordinates import Translation, AtlasCoordinate, AtlasLibrary, CoordinateSystemLibrary
from aind_data_schema_models.units import TimeUnit
from aind_data_schema_models.stimulus_modality import StimulusModality
from aind_data_schema.components.identifiers import Software, Code
from aind_data_schema.core.acquisition import StimulusEpoch
from mindscope_to_nwb_zarr.aind_data_schema.stimuli import OptotaggingStimulation

from datetime import datetime, timezone, timedelta
from pynwb import NWBFile


def get_subject_id(nwbfile: NWBFile, session_info: pd.DataFrame = None) -> str:
    """Get the subject ID from the NWB file, cross-checked with the session info. e.g., "457841".
    """
    if session_info is not None:
        assert session_info['mouse_id'].values[0] == int(nwbfile.subject.subject_id), "subject_id mismatch occurred"
    return nwbfile.subject.subject_id


def get_subject_date_of_birth(nwbfile: NWBFile) -> datetime.date:
    """Calculate the animal's date of birth from age and acquisition date in NWB file.
    """
    # Extract age in days from NWB file subject.age field
    age_str = nwbfile.subject.age
    match = re.match(r'P(\d+)D', age_str)
    if not match:
        raise ValueError(f"Unable to parse age from NWB file. Expected format 'P<days>D', got '{age_str}'")

    age_in_days = int(match.group(1))

    # Calculate date of birth by subtracting age from acquisition date
    acquisition_datetime = nwbfile.session_start_time
    date_of_birth = (acquisition_datetime - timedelta(days=age_in_days)).date()

    return date_of_birth


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


def get_brain_locations(nwbfile: NWBFile, device) -> list[CCFv3]:
    """Convert location names to CCFv3 brain structures.

    For VIS regions that don't have an exact match in CCFv3, this function
    falls back to the generic VIS structure.
    """
    # get locations from nwbfile
    locations = (nwbfile.electrodes.to_dataframe()
                .query('group_name == @device.name')['location'].unique().tolist())
    locations = [l for l in locations if l]  # Filter out empty strings

    # extract CCFv3 structures
    all_structures = []
    for location in locations:
        location_upper = location.upper()
        structure = getattr(CCFv3, location_upper, None)

        if structure is not None:
            all_structures.append(structure)
        else:
            pass

    # warn if missing any
    if len(all_structures) != len(locations):
        warnings.warn(f"All probe locations not found in CCFv3 enum: {locations}")

    return all_structures


def get_probe_configs(nwbfile: NWBFile) -> list[ProbeConfig]:
    """Get probe configurations from NWB file.

    Extracts probe information including targeted brain structures from the NWB file's
    electrode table and device information.

    Parameters
    ----------
    nwbfile : NWBFile
        The NWB file containing probe and electrode information

    Returns
    -------
    list[ProbeConfig]
        List of probe configuration objects for each probe in the file
    """
    probe_configs = []
    all_targeted_structures = []
    for device in nwbfile.devices.values():
        if device.__class__.__name__ == "EcephysProbe":
            all_structures = get_brain_locations(nwbfile, device)
            targeted_structure = [s for s in all_structures if s.acronym.startswith('VIS')]  # get targeted visual area
            if len(targeted_structure) > 1:
                # NOTE: visual coding dataset has 12 functionally defined visual areas that are not included in the CCFv3
                # these regions will be ignored and the target structure should still fall under one of the 6 VIS areas
                # VISal, VISam, VISl, VISpl, VISp, VISrl, VISpm
                warnings.warn(f"More than one visual area found: {targeted_structure}")

            all_targeted_structures.append(targeted_structure[0])

            probe_configs.append(
                ProbeConfig(
                    device_name=device.name,
                    # 6 probes, each targets a cortical visual area (e.g. VISp, VISl, VISal, VISrl, VISam, VISpm)
                    # would list that specific area as the primary targeted structure
                    # should be the same for every experiment, most files should have majority of one
                    primary_targeted_structure=targeted_structure[0],
                    other_targeted_structure=list(set(all_structures) - set(targeted_structure)), # TODO - currently listing all other structures that are hit but might want to not list everything
                    atlas_coordinate=AtlasCoordinate(
                        coordinate_system=AtlasLibrary.CCFv3_10um,
                        translation=[0, 0, 0],  # TODO - should be target region coordinate - might not make sense for these datasets, TBD @Saskia
                    ),
                    coordinate_system=CoordinateSystemLibrary.MPM_MANIP_RFB,  # TODO - what should this be? probably bregma ARID, will confirm
                    transform=[Translation(translation=[0, 0, 0, 1],),],  # TODO - what should this be? this will be the translation we care about, how we've positioned this probe
                    # expect that there is documentation on these translations somewhere @Saskia
                    notes=None,
                )
            )

    if len(set(all_targeted_structures)) != len(all_targeted_structures):
        warnings.warn(f"Multiple probes targeting same brain structure found: {all_targeted_structures}")

    return probe_configs


def get_optostimulation_parameters(optogenetic_stimulation) -> dict[str, OptotaggingStimulation]:
    """Extract optogenetic stimulation parameters from NWB optotagging data.

    Parameters
    ----------
    optogenetic_stimulation : TimeIntervals
        The optogenetic stimulation time intervals from the NWB file

    Returns
    -------
    dict[str, OptotaggingStimulation]
        Dictionary mapping stimulus names to OptotaggingStimulation objects
    """
    opto_stimulation = dict()
    opto_df = optogenetic_stimulation.to_dataframe()
    for stimulus_name, df in opto_df.groupby('stimulus_name'):
        assert len(df['condition'].unique()) == 1, "Multiple pulse shapes found for stimulus_name"
        if 'pulse' in df['condition'].values[0].lower():
            pulse_shape = PulseShape.SQUARE  # TODO - double check if this is best descriptor for both slow and fast pulses
        elif 'cosine' in df['condition'].values[0].lower():
            pulse_shape = PulseShape.RAMP  # TODO - described as "raised cosine ramp" in whitepaper, could define new enum if needed
        else:
            raise ValueError(f"Unknown pulse shape in condition: {df['condition'].values[0]}")

        # get pulse duration and light levels used
        light_levels = sorted(df['level'].unique().tolist())
        pulse_durations = df['duration'].unique()

        # create custom OptotaggingStimulation model
        opto_stimulation[stimulus_name] = OptotaggingStimulation(
            stimulus_name=stimulus_name,
            pulse_shape=pulse_shape,
            pulse_durations=[np.round(p, 10) for p in pulse_durations],
            pulse_durations_unit=TimeUnit.S,
            ramp_duration=0.0005, # from technical whitepaper
            ramp_duration_unit=TimeUnit.S,
            inter_pulse_interval=1.5,
            inter_pulse_interval_unit=TimeUnit.S,
            inter_pulse_interval_delay_range=(0, 0.5),
            inter_pulse_interval_delay_range_unit=TimeUnit.S,
            light_levels=light_levels,
            condition_description=df['condition'].values[0],
        )

    return opto_stimulation


def get_visual_stimulation_parameters(table_key: str, intervals_table: pd.DataFrame) -> VisualStimulation:
    """Extract visual stimulation parameters from an intervals table.

    Parameters
    ----------
    table_key : str
        The name of the intervals table
    intervals_table : pd.DataFrame
        DataFrame containing the stimulus presentation intervals

    Returns
    -------
    VisualStimulation
        Visual stimulation object with extracted parameters
    """
    # TODO - determine if there are any other parameters to include or better units
    possible_parameters_and_units = {
        "orientation": "degrees",
        "spatial_frequency": "cycles/degree",
        "temporal_frequency": "Hz",
        "contrast": "percent",
        "duration": "S",
        "phase": None,
        "size": None,
        "image_name": None,
        "image_set": None,
        "stimulus_name": None,
        "stimulus_block": None,
        "color": None,
        "opacity": None,
        "mask": None,
        "speed": "degrees/second",
        "dir": "degrees",
        "coherence": "percent",
        "dotLife": None,
        "dotSize": None,
        "nDots": None,
        "fieldPos": None,
        "fieldShape": None,
        "fieldSize": None,
    }
    parameters = {}
    for param_key, param_unit in possible_parameters_and_units.items():
        if param_key in intervals_table.columns:
            parameter_values = intervals_table[param_key].unique().tolist()
            parameter_values = parameter_values[0] if len(parameter_values) == 1 else parameter_values
            parameters.update({param_key: parameter_values})
            if param_unit is not None:
                parameters.update({f"{param_key}_unit": param_unit})

    visual_stimulation = VisualStimulation(
        stimulus_name=table_key,
        stimulus_parameters=parameters,
        stimulus_template_name=intervals_table['stimulus_name'].unique().tolist(),
        notes=None,
    )
    return visual_stimulation


def convert_intervals_to_stimulus_epochs(stimulus_name: str, table_key: str, intervals_table: pd.DataFrame,
                                         nwbfile: NWBFile, session_info: pd.DataFrame = None) -> StimulusEpoch:
    """Convert intervals table to a StimulusEpoch object.

    Parameters
    ----------
    stimulus_name : str
        Name of the stimulus
    table_key : str
        Key for the intervals table
    intervals_table : pd.DataFrame
        DataFrame containing stimulus presentation intervals
    nwbfile : NWBFile
        The NWB file containing session information
    session_info : pd.DataFrame, optional
        DataFrame with session metadata (for visual behavior experiments)

    Returns
    -------
    StimulusEpoch
        Stimulus epoch object with extracted parameters
    """
    return StimulusEpoch(
        stimulus_start_time=timedelta(seconds=intervals_table['start_time'].values[0]) + nwbfile.session_start_time,
        stimulus_end_time=timedelta(seconds=intervals_table['stop_time'].values[-1]) + nwbfile.session_start_time,
        stimulus_name=stimulus_name,
        # TODO - acquire additional info about the code used for this task - might not be available
        # will need to fill in with some type of information so we can use the Code.parameters field @Saskia
        code=Code(
            url="None",
            core_dependency=Software(
                name="PsychoPy",
                version=None,
            ),  # TODO - from whitepaper, add version if available @Saskia
            parameters=get_visual_stimulation_parameters(table_key, intervals_table).model_dump(),
        ),
        stimulus_modalities=[StimulusModality.VISUAL],
        notes=None,
        active_devices=["None"],
        performance_metrics=None,  # TODO - see if these are accessible anywhere?
        training_protocol_name=session_info["session_type"].values[0] if session_info is not None else None,  # e.g., "TRAINING_0_gratings_autorewards_15min"
        curriculum_status=get_curriculum_status(session_info) if session_info is not None else None,
    )


def serialized_dict(**kwargs) -> str:
    return json.dumps(dict(**kwargs), cls=NumpyJsonEncoder)


class NumpyJsonEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle NumPy data types."""
    def default(self, obj):
        if isinstance(obj, (np.integer, np.floating, np.ndarray)):
            return obj.tolist()
        return super().default(obj)