"""Generates an example JSON file for visual behavior ephys acquisition"""

import numpy as np
import warnings
from datetime import timedelta

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

import pandas as pd
from pynwb import read_nwb
from pathlib import Path
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
# TODO - test with pure behavior files
# TODO - add performance metrics for behavior if available
# TODO - fill in additional missing sections
subject_id = 506940
session_id = 1043752325

# Get the repository root (3 levels up from this file)
repo_root = Path(__file__).parent.parent.parent.parent
cache_dir = repo_root / ".cache/visual_behavior_neuropixels_cache_dir/visual-behavior-neuropixels-0.5.0/project_metadata/"

# load nwb files
nwbfile_lfp = read_nwb(repo_root / f"data/sub-{subject_id}_ses-None_probe-1158270876_ecephys.nwb")
nwbfile = read_nwb(repo_root / f"data/sub-{subject_id}_ses-20200817T222149.nwb")

# load metadata files
ephys_session_table = pd.read_csv(cache_dir / "ecephys_sessions.csv")
behavior_session_table = pd.read_csv(cache_dir / "behavior_sessions.csv")
session_info = ephys_session_table.query("mouse_id == @subject_id and ecephys_session_id == @session_id")
behavior_session_info = behavior_session_table.query("mouse_id == @subject_id and ecephys_session_id == @session_id")
if len(session_info) == 0 and len(behavior_session_info) == 1:
    warnings.warn("Session info only found for behavioral data - defaulting to behavior only session")
elif len(session_info) == 0 and len(behavior_session_info) == 0:
    raise ValueError(f"No ephys session info found for subject_id={subject_id}, session_id={session_id}")

def get_probe_configs(nwbfile):
    probe_configs = []
    for device in nwbfile.devices.values():
        if device.__class__.__name__ == "EcephysProbe":
            locations = (nwbfile.electrodes.to_dataframe()
                         .query('group_name == @device.name')['location'].unique().tolist())
            targeted_structures = [getattr(CCFv3, l.upper()) for l in locations if getattr(CCFv3, l.upper(), None) is not None]
            assert len(targeted_structures) == len(locations), "All probe locations not found in CCFv3 enum"

            probe_configs.append(
                ProbeConfig(
                    device_name=device.name,
                    primary_targeted_structure=CCFv3.VIS, # TODO - update if need to be more specific
                    other_targeted_structure=targeted_structures,
                    atlas_coordinate=AtlasCoordinate(
                        coordinate_system=AtlasLibrary.CCFv3_10um,
                        translation=[0, 0, 0], # TODO - should be target region coordinate
                    ),
                    coordinate_system=CoordinateSystemLibrary.MPM_MANIP_RFB, # TODO - what should this be?
                    transform=[Translation(translation=[0, 0, 0, 1],),], # TODO - what should this be?
                    notes=None,
                )
            )
    
    return probe_configs

def get_optostimulation_parameters(optogenetic_stimulation):
    opto_stimulation = dict()
    opto_df = optogenetic_stimulation.to_dataframe()
    for stimulus_name, df in opto_df.groupby('stimulus_name'):
        assert len(df['condition'].unique()) == 1, "Multiple pulse shapes found for stimulus_name"
        if 'square' in df['condition'].values[0]:
            pulse_shape = PulseShape.SQUARE
        elif 'cosine' in df['condition'].values[0]:
            pulse_shape = PulseShape.SINE # TODO - also described as "raised cosine ramp" in whitepaper
        
        # convert mean intervals to frequency
        pulse_frequency = 1.0 / np.mean(np.diff(df['start_time'])) 

        opto_stimulation[stimulus_name] = (
            OptoStimulation(
                stimulus_name=stimulus_name,
                pulse_shape=pulse_shape,
                pulse_frequency=[pulse_frequency], # TODO - what should this be?
                pulse_frequency_unit=FrequencyUnit.HZ,
                number_pulse_trains=[len(df)], # TODO- what should this be?
                pulse_width=(df['duration'].unique() * 100).astype(int).tolist(),
                pulse_width_unit=TimeUnit.MS,
                pulse_train_duration=(df['duration'].unique() * 100).astype(int).tolist(), # TODO - what should this be?
                pulse_train_interval=1.5, # from technical whitepaper
                fixed_pulse_train_interval=False, # from technical whitepaper
                pulse_train_interval_unit=TimeUnit.S,
                baseline_duration=0.0, # TODO - is whole prior recording considered baseline? add if needed
                baseline_duration_unit=TimeUnit.S,
                notes=f"{df['condition'].values[0]} with three light levels: {df['level'].unique().tolist()}",
            )
        )

    return opto_stimulation

def get_visual_stimulation_prameters(table_key: str, intervals_table: pd.DataFrame) -> VisualStimulation:
    # TODO - better way to select for different parameter options, see if there are any others to include
    if "gabor" in table_key:
        parameters = {"orientations": intervals_table['orientation'].unique().tolist(),
                      "orientation_unit": "degrees",
                      "spatial_frequencies": intervals_table['spatial_frequency'].unique().tolist(),
                      "spatial_frequency_unit": "cycles/degree",
                      "temporal_frequencies": intervals_table['temporal_frequency'].unique().tolist(),
                      "temporal_frequency_unit": "Hz",
                      "contrasts": intervals_table['contrast'].unique().tolist(),
                      "contrast_unit": "percent",
                      "durations": intervals_table['duration'].unique().tolist(),
                      "duration_unit": "S"
        }
    elif "flash" in table_key:
        parameters = {"contrasts": intervals_table['contrast'].unique().tolist(),
                      "contrast_unit": "percent",
                      "durations": intervals_table['duration'].unique().tolist(),
                      "duration_unit": "S"
            
        }
    elif "Natural_Images" in table_key:
        parameters = {"mean_luminance": 50, # from technical whitepaper
                      "mean_luminance_units": "cd/m2",
                      "durations": intervals_table['duration'].unique().tolist(),
                      "duration_unit": "S",
                      "image_names": intervals_table["image_name"].unique().tolist()
                      }
    elif "spontaneous" in table_key:
        parameters = {"durations": intervals_table['duration'].unique().tolist(),
                      "duration_unit": "S"
                      }
    else:
        parameters = {}

    visual_stimulation = VisualStimulation(
                            stimulus_name=table_key,
                            stimulus_parameters=parameters,
                            stimulus_template_name=["None"],
                            notes=None,
                        )
    return visual_stimulation

def get_stimulation_epochs(nwbfile):
    stimulation_epochs = []
    stimulation_mapping = {"Active behavior": "Natural_Images_Lum_Matched_set_ophys_G_2019_presentations",
                           "Gabors": "gabor_20_deg_250ms_presentations",
                           "Spontaneous": "spontaneous_presentations",
                           "Passive replay": "Natural_Images_Lum_Matched_set_ophys_G_2019_presentations",
                           "Full-field flashes": "flash_250ms_presentations",}

    for stimulus_name, table_key in stimulation_mapping.items():
        # split active and passive sessions into two stimulation epochs
        if table_key == "Natural_Images_Lum_Matched_set_ophys_G_2019_presentations" and stimulus_name == "Active behavior":
            intervals_table_filtered = (nwbfile.intervals[table_key].to_dataframe()
                                        .query('active == True'))
        elif table_key == "Natural_Images_Lum_Matched_set_ophys_G_2019_presentations" and stimulus_name == "Passive replay":
            intervals_table_filtered = (nwbfile.intervals[table_key].to_dataframe()
                                        .query('active == False'))
        else:
            intervals_table_filtered = nwbfile.intervals[table_key].to_dataframe()

        stim_epoch = StimulusEpoch(
                stimulus_start_time=timedelta(seconds=intervals_table_filtered['start_time'].values[0]) + nwbfile.session_start_time,
                stimulus_end_time=timedelta(seconds=intervals_table_filtered['stop_time'].values[-1]) + nwbfile.session_start_time,
                stimulus_name=stimulus_name,
                code=Code( # TODO - acquire additional info about the code used for this task
                    url="None",
                    name="None",
                    version="None",
                    container=None,
                    run_script=None,
                    language=None,
                    language_version=None,
                    input_data=None,
                    core_dependency=Software(
                        name="PsychoPy",
                        version=None,), # TODO - add software version if available
                    parameters=get_visual_stimulation_prameters(table_key, intervals_table_filtered),
                ),
                stimulus_modalities=[StimulusModality.VISUAL],
                performance_metrics=None, # TODO - see if these are accessible anywhere?
                notes=None,
                active_devices=["None"],
                training_protocol_name=session_info["session_type"].values[0],  # e.g., "TRAINING_0_gratings_autorewards_15min"
                curriculum_status=get_curriculum_status(session_info),
            )
        stimulation_epochs.append(stim_epoch)
    
    if 'optotagging' in nwbfile.processing:
        optogenetic_stimulation = nwbfile.processing['optotagging']['optogenetic_stimulation']
        opto_stim_epoch = StimulusEpoch(
            stimulus_start_time=timedelta(seconds=optogenetic_stimulation['start_time'][0]) + nwbfile.session_start_time,
            stimulus_end_time=timedelta(seconds=optogenetic_stimulation['stop_time'][-1]) + nwbfile.session_start_time,
            stimulus_name="Optotagging",
            code=Code( # TODO - add code source if available
                url="None",
                name="None",
                version="None",
                container=None,
                run_script=None,
                language=None,
                language_version=None,
                input_data=None,
                core_dependency=Software(
                    name="None",
                    version=None,), # TODO - add software if available
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
    specimen_id=None, # TODO - confirm whether necessary for in vivo ephys files
    acquisition_start_time=get_session_start_time(nwbfile, session_info=session_info),
    acquisition_end_time=get_data_stream_end_time(nwbfile),
    protocol_id=None, # TODO - confirm whether shared on protocols.io
    ethics_review_id=None, #TODO - obtain if available
    instrument_id=get_instrument_id(nwbfile, session_info=session_info),
    acquisition_type=nwbfile.session_description, # TODO - confirm consistent across experiments or if better option
    notes=None,
    coordinate_system=CoordinateSystemLibrary.BREGMA_ARID, # TODO - determine correct coordinate system library
    # calibrations=[], # TODO - add if available
    # maintenance=[],
    data_streams=[ # TODO - confirm single stream of ephys + behavior data is appropriate
        DataStream(
            stream_start_time=get_data_stream_start_time(nwbfile),
            stream_end_time=get_data_stream_end_time(nwbfile),
            modalities=get_modalities(nwbfile),
            code=None,
            notes=None,
            active_devices=[ # TODO - determine all active devices names that would apply and their names
                "EPHYS_1", # TODO - add conditional for behavioral data to select appropriate devices
                "Laser_1",
                "Lick_Spout_1",
            ],
            configurations=[
                EphysAssemblyConfig(
                    device_name="EPHYS_1",
                    manipulator=ManipulatorConfig(
                        device_name="Manipulator_1", # TODO - fill in with correct information
                        coordinate_system=CoordinateSystemLibrary.MPM_MANIP_RFB,
                        local_axis_positions=Translation(translation=[0, 0, 0],), # TODO - fill in with correct positions
                    ),
                    probes=get_probe_configs(nwbfile),
                ),
                LaserConfig( # TODO - should this go here or in the stimulation epochs configuration field?
                    device_name="Laser_1",
                    wavelength=473, # from technical whitepaper
                    wavelength_unit=SizeUnit.NM,
                ),
                LickSpoutConfig(
                    device_name="Lick_Spout_1",
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
        animal_weight_prior=None, # TODO - pull in extra info if available
        animal_weight_post=None,
        weight_unit=MassUnit.G,
        anaesthesia=None,
        mouse_platform_name="Running Wheel",
        reward_consumed_total=get_total_reward_volume(nwbfile), # TODO - check if calculation is ok
        reward_consumed_unit=VolumeUnit.ML
    ),
)


if __name__ == "__main__":
    serialized = acquisition.model_dump_json()
    deserialized = Acquisition.model_validate_json(serialized)
    deserialized.write_standard_file(prefix="ephys_visual_behavior")