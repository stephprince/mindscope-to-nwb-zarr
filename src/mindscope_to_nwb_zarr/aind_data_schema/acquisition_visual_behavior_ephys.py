"""Generates an example JSON file for visual behavior ephys acquisition"""

import warnings
import numpy as np
import pandas as pd

from datetime import timedelta
from pathlib import Path
from pynwb import read_nwb

from aind_data_schema.components.identifiers import Software, Code
from aind_data_schema.core.acquisition import (
    Acquisition,
    StimulusEpoch,
    DataStream,
    AcquisitionSubjectDetails,
)
from aind_data_schema.components.configs import (
    ManipulatorConfig,
    EphysAssemblyConfig,
    ProbeConfig,
    LaserConfig,
    LickSpoutConfig,
    Liquid,
    Valence,
)
from aind_data_schema.components.coordinates import (
    Translation,
    AtlasCoordinate,
    AtlasLibrary,
    CoordinateSystemLibrary,
)
from aind_data_schema.components.stimulus import VisualStimulation, OptoStimulation, PulseShape
from aind_data_schema_models.units import TimeUnit, SizeUnit, VolumeUnit, FrequencyUnit, MassUnit
from aind_data_schema_models.brain_atlas import CCFv3
from aind_data_schema_models.stimulus_modality import StimulusModality

from mindscope_to_nwb_zarr.pynwb_utils import (
    get_data_stream_start_time,
    get_data_stream_end_time, 
    get_modalities
)
from mindscope_to_nwb_zarr.aind_data_schema.utils import (
    get_subject_id,
    get_session_start_time,
    get_instrument_id,
    get_total_reward_volume,
    get_individual_reward_volume,
    get_curriculum_status,
)

# example file for initial debugging
# TODO - replace with more general ingestion/generation script

behavior_only = False # set to True to test with behavior only session
repo_root = Path(__file__).parent.parent.parent.parent
cache_dir = repo_root / ".cache/visual_behavior_neuropixels_cache_dir/visual-behavior-neuropixels-0.5.0/project_metadata/"

subject_id = 506940
if behavior_only:
    session_id = 1014008383
    nwbfile = read_nwb(repo_root / f"data/behavior_session_{session_id}.nwb")
else:
    session_id = 1043752325
    nwbfile = read_nwb(repo_root / f"data/sub-{subject_id}_ses-20200817T222149.nwb")
    # nwbfile = read_nwb(repo_root / f"data/ecephys_session_{session_id}.nwb")

# load metadata files
ephys_session_table = pd.read_csv(cache_dir / "ecephys_sessions.csv")
behavior_session_table = pd.read_csv(cache_dir / "behavior_sessions.csv")
session_info = ephys_session_table.query("mouse_id == @subject_id and ecephys_session_id == @session_id")
behavior_session_info = behavior_session_table.query("mouse_id == @subject_id and behavior_session_id == @session_id")
if len(session_info) == 0 and len(behavior_session_info) == 1:
    warnings.warn("Session info only found for behavioral data - defaulting to behavior only session")
    session_info = behavior_session_info
assert nwbfile.session_description == session_info['session_type'].values[0]

def get_probe_configs(nwbfile):
    probe_configs = []
    all_targeted_structures = []
    for device in nwbfile.devices.values():
        if device.__class__.__name__ == "EcephysProbe":
            locations = (nwbfile.electrodes.to_dataframe()
                         .query('group_name == @device.name')['location'].unique().tolist())
            all_structures = [getattr(CCFv3, l.upper()) for l in locations if getattr(CCFv3, l.upper(), None) is not None]
            targeted_structure = [s for s in all_structures if s.acronym.startswith('VIS')] # get targeted visual area
            assert len(all_structures) == len(locations), "All probe locations not found in CCFv3 enum"
            assert len(targeted_structure) == 1, "More than one visual area found"
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
                        translation=[0, 0, 0], # TODO - should be target region coordinate - might not make sense for these datasets, TBD @Saskia
                    ),
                    coordinate_system=CoordinateSystemLibrary.MPM_MANIP_RFB, # TODO - what should this be? probably bregma ARID, will confirm
                    transform=[Translation(translation=[0, 0, 0, 1],),], # TODO - what should this be? this will be the translation we care about, how we've positioned this probe
                    # expect that there is documentation on these translations somewhere @Saskia
                    notes=None,
                )
            )
    assert len(set(all_targeted_structures)) == len(all_targeted_structures), "Duplicate targeted structures found across probes"
    
    return probe_configs

def get_optostimulation_parameters(optogenetic_stimulation):
    opto_stimulation = dict()
    opto_df = optogenetic_stimulation.to_dataframe()
    for stimulus_name, df in opto_df.groupby('stimulus_name'):
        assert len(df['condition'].unique()) == 1, "Multiple pulse shapes found for stimulus_name"
        if 'square' in df['condition'].values[0]:
            pulse_shape = PulseShape.SQUARE
        elif 'cosine' in df['condition'].values[0]:
            pulse_shape = PulseShape.SINE # TODO - also described as "raised cosine ramp" in whitepaper, may need to define new enum
        
        # convert mean intervals to frequency
        pulse_frequency = 1.0 / np.mean(np.diff(df['start_time'])) 

        opto_stimulation[stimulus_name] = (
            # TODO - replace this with a new stimulation class inspired by this one with appropriate set of parameters / fields
            OptoStimulation(
                stimulus_name=stimulus_name,
                pulse_shape=pulse_shape,
                pulse_frequency=[pulse_frequency], # TODO - remove in custom class
                pulse_frequency_unit=FrequencyUnit.HZ,
                number_pulse_trains=[len(df)], # TODO - remove in custom class
                pulse_width=(df['duration'].unique() * 100).astype(int).tolist(),
                pulse_width_unit=TimeUnit.MS,
                pulse_train_duration=(df['duration'].unique() * 100).astype(int).tolist(), # TODO - remove in custom class
                pulse_train_interval=1.5, # from technical whitepaper
                fixed_pulse_train_interval=False, # from technical whitepaper
                pulse_train_interval_unit=TimeUnit.S,
                baseline_duration=0.0, # TODO - remove in custom class
                baseline_duration_unit=TimeUnit.S,
                notes=f"{df['condition'].values[0]} with three light levels: {df['level'].unique().tolist()}",
            )
        )

    return opto_stimulation

def get_visual_stimulation_parameters(table_key: str, intervals_table: pd.DataFrame) -> VisualStimulation:
    # TODO - determine if there are any other parameters to include
    # NOTE - parameter serialization to JSON is not showing up correctly, also an issue in the example file, submit an issue to fix
    possible_parameters_and_units = {"orientation": "degrees",
                                     "spatial_frequency": "cycles/degree",
                                     "temporal_frequency": "Hz",
                                     "contrast": "percent",
                                     "duration": "S",
                                     "phase": None,
                                     "image_name": None,
                                     "image_set": None,}
    parameters = {}
    for param_key, param_unit in possible_parameters_and_units.items():
        if param_key in intervals_table.columns:
            parameters.update({param_key: intervals_table[param_key].unique().tolist()})
            if param_unit is not None:
                parameters.update({f"{param_key}_unit": param_unit})

    visual_stimulation = VisualStimulation(
                            stimulus_name=table_key,
                            stimulus_parameters=parameters,
                            stimulus_template_name=intervals_table['stimulus_name'].unique().tolist(),
                            notes=None,
                        )
    return visual_stimulation

def convert_intervals_to_stimulus_epochs(stimulus_name: str, table_key: str, intervals_table: pd.DataFrame) -> StimulusEpoch:
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
                    ), # TODO - from whitepaper, add version if available @Saskia
                    parameters=get_visual_stimulation_parameters(table_key, intervals_table),
                ),
                stimulus_modalities=[StimulusModality.VISUAL],
                performance_metrics=None, # TODO - see if these are accessible anywhere?
                notes=None,
                active_devices=["None"],
                training_protocol_name=session_info["session_type"].values[0],  # e.g., "TRAINING_0_gratings_autorewards_15min"
                curriculum_status=get_curriculum_status(session_info),
            )

def get_stimulation_epochs(nwbfile):
    # loop through all intervals tables
    stimulation_epochs = []
    stimulation_type = ["Gabor", "Spontaneous", "Passive replay", "Flash", "Grating"] # TODO - determine if any other types to consider
    for table_key, intervals_table in nwbfile.intervals.items():
        # skip generic trials table that contains behavioral data
        if table_key == "trials":
            continue
        # split active and passive behavior sessions into different stimulus epochs
        elif table_key == "Natural_Images_Lum_Matched_set_ophys_G_2019_presentations":
            active_intervals = intervals_table.to_dataframe().query('active == True')
            stimulus_name = "Change detection - Active"
            stim_epoch = convert_intervals_to_stimulus_epochs(stimulus_name=stimulus_name, 
                                                            table_key=table_key, 
                                                            intervals_table=active_intervals)
            stimulation_epochs.append(stim_epoch)

            passive_intervals = intervals_table.to_dataframe().query('active == False')
            stimulus_name = "Change detection - Passive replay"
            stim_epoch = convert_intervals_to_stimulus_epochs(stimulus_name=stimulus_name, 
                                                            table_key=table_key, 
                                                            intervals_table=passive_intervals)
            stimulation_epochs.append(stim_epoch)
        else:
            intervals_table_filtered = intervals_table.to_dataframe()
            stimulus_name = next((stim for stim in stimulation_type if stim.lower() in table_key), None)
            assert stimulus_name is not None, f"Associated stimulus type for intervals table was not found"

            stim_epoch = convert_intervals_to_stimulus_epochs(stimulus_name=stimulus_name, 
                                                              table_key=table_key, 
                                                              intervals_table=intervals_table_filtered)

            stimulation_epochs.append(stim_epoch)
    
    if 'optotagging' in nwbfile.processing:
        optogenetic_stimulation = nwbfile.processing['optotagging']['optogenetic_stimulation']
        opto_stim_epoch = StimulusEpoch(
            stimulus_start_time=timedelta(seconds=optogenetic_stimulation['start_time'][0]) + nwbfile.session_start_time,
            stimulus_end_time=timedelta(seconds=optogenetic_stimulation['stop_time'][-1]) + nwbfile.session_start_time,
            stimulus_name="Optotagging",
            code=Code( # TODO - add code source if available
                url="None",
                parameters=get_optostimulation_parameters(optogenetic_stimulation),
            ),
            stimulus_modalities=[StimulusModality.OPTOGENETICS],
            performance_metrics=None,
            notes=None,
            active_devices=["Laser_1"],
            configurations=[LaserConfig(
                    device_name="Laser_1",
                    wavelength=473, # from technical whitepaper
                    wavelength_unit=SizeUnit.NM,
                ),
            ],
            training_protocol_name=None,
            curriculum_status=None,
        )
        stimulation_epochs.append(opto_stim_epoch)

    return stimulation_epochs

acquisition = Acquisition(
    subject_id=get_subject_id(nwbfile, session_info=session_info),
    acquisition_start_time=get_session_start_time(nwbfile, session_info=session_info),
    acquisition_end_time=get_data_stream_end_time(nwbfile),
    ethics_review_id=None, #TODO - obtain if available - YES, @Saskia
    instrument_id=get_instrument_id(nwbfile, session_info=session_info),
    acquisition_type=nwbfile.session_description,
    notes=None,
    coordinate_system=CoordinateSystemLibrary.BREGMA_ARID, # TODO - determine correct coordinate system library, will also be defined with instrument (not required to be same as acquisition)
    # coordinate system info might not be available, will check @Saskia
    # calibrations=[], # TODO - add if available - will be difficult to find, probably not
    # maintenance=[],
    data_streams=[
        DataStream(
            stream_start_time=get_data_stream_start_time(nwbfile),
            stream_end_time=get_data_stream_end_time(nwbfile),
            modalities=get_modalities(nwbfile),
            code=None,
            notes=None,
            # active devices will be placeholders depending on the instrument information getting filled in
            # configurations will also be dependent on instrument information 
            # TODO - wait for instrument information but could maybe get some placeholders for active device names @Saskia
            active_devices=[
                "EPHYS_1", # TODO - add conditional for behavioral data to select appropriate devices
                "Laser_1",
                "Lick_Spout_1",
            ],
            configurations=[
                EphysAssemblyConfig(
                    device_name="EPHYS_1",
                    manipulator=ManipulatorConfig(
                        device_name="Manipulator_1", # TODO - fill in with correct information
                        coordinate_system=CoordinateSystemLibrary.MPM_MANIP_RFB, # should be standardized (confirm relative to bregma, positions) @Saskia
                        local_axis_positions=Translation(translation=[0, 0, 0],), # TODO - fill in with correct positions @Saskia
                    ),
                    probes=get_probe_configs(nwbfile),
                ),
                LaserConfig( # TODO - should this go here or in the stimulation epochs configuration field?
                    device_name="Laser_1", # placeholder
                    wavelength=473, # from technical whitepaper
                    wavelength_unit=SizeUnit.NM,
                ),
                LickSpoutConfig(
                    device_name="Lick_Spout_1", # placeholder
                    solution=Liquid.WATER,
                    solution_valence=Valence.POSITIVE,
                    volume=get_individual_reward_volume(nwbfile),
                    volume_unit=VolumeUnit.ML,
                    relative_position=["Anterior"], # TODO - what is the correct information here? 
                )
                # TODO - add information about Monitor, Camera, LED from nwbfile.processing['eye_tracking_rig_metadata']['eye_tracking_rig_metadata']
                # TODO - should we add MousePlatformConfig here too?
            ],
         ),
    ],
    stimulus_epochs=get_stimulation_epochs(nwbfile),
    subject_details=AcquisitionSubjectDetails(
        animal_weight_prior=None, # TODO - pull in extra info if available - likely not available @Saskia
        animal_weight_post=None,
        weight_unit=MassUnit.G,
        anaesthesia=None,
        mouse_platform_name="Running Wheel",
        reward_consumed_total=get_total_reward_volume(nwbfile),
        reward_consumed_unit=VolumeUnit.ML
    ),
)


if __name__ == "__main__":
    serialized = acquisition.model_dump_json()
    deserialized = Acquisition.model_validate_json(serialized)
    deserialized.write_standard_file(prefix=repo_root / f"data/schema/ephys_visual_behavior_{session_id}")
