"""Generates an example JSON file for visual behavior ephys acquisition"""

import numpy as np

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
    Rotation,
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
)

# example file for initial debugging
# TODO - replace with more general ingestion/generation script
subject_id = 506940
session_id = 1043752325
working_dir = Path("/Users/stephprince/Documents/code/mindscope-to-nwb-zarr/")
cache_dir = working_dir / ".cache/visual_behavior_neuropixels_cache_dir/visual-behavior-neuropixels-0.5.0/project_metadata/"

# load nwb files
nwbfile_lfp = read_nwb(working_dir / f"data/sub-{subject_id}_ses-None_probe-1158270876_ecephys.nwb")
nwbfile = read_nwb(working_dir / f"data/sub-{subject_id}_ses-20200817T222149.nwb")

# load metadata files
ephys_session_table = pd.read_csv(cache_dir / "ecephys_sessions.csv")
session_info = ephys_session_table.query("mouse_id == @subject_id and ecephys_session_id == @session_id")
if len(session_info) == 0:
    raise ValueError(f"No session info found for subject_id={subject_id}, session_id={session_id}")

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
            pulse_shape = PulseShape.SINE # TODO - also described as raised cosine ramp in whitepaper
        
        pulse_frequency = 1.0 / np.mean(np.diff(df['start_time'])) # convert mean intervals to frequency

        opto_stimulation[stimulus_name] = (
            OptoStimulation(
                stimulus_name=stimulus_name,
                pulse_shape=pulse_shape,
                pulse_frequency=[pulse_frequency], # TODO - is this the correct interpretation
                pulse_frequency_unit=FrequencyUnit.HZ,
                number_pulse_trains=[len(df)], # TODO - is this the correct interpretation
                pulse_width=(df['duration'].unique() * 100).astype(int).tolist(),
                pulse_width_unit=TimeUnit.MS,
                pulse_train_duration=(df['duration'].unique() * 100).astype(int).tolist(), # TODO - is this the correct interpretation
                pulse_train_interval=1.5, # from technical whitepaper
                fixed_pulse_train_interval=False, # from technical whitepaper
                pulse_train_interval_unit=TimeUnit.S,
                baseline_duration=0.0, # TODO - is whole prior recording considered baseline? add if needed
                baseline_duration_unit=TimeUnit.S,
                notes=f"{pulse_shape} with three light levels: {df['level'].unique().tolist()}",
            )
        )

    return opto_stimulation

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
                stimulus_start_time=intervals_table_filtered['start_time'].values[0],
                stimulus_end_time=intervals_table_filtered['stop_time'].values[-1],
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
                        name="None",
                        version=None,), # TODO - add software if available
                    parameters=VisualStimulation(
                        stimulus_name=table_key,
                        stimulus_parameters={ # TODO - make these accurate
                            "grating_orientations": [0, 45, 90, 135],
                            "grating_orientation_unit": "degrees",
                            "grating_spatial_frequencies": [0.02, 0.04, 0.08, 0.16, 0.32],
                            "grating_spatial_frequency_unit": "cycles/degree",
                        },
                        stimulus_template_name=["None"],
                        notes=None,
                    ),
                ),
                stimulus_modalities=[StimulusModality.VISUAL],
                performance_metrics=None, # TODO - see if we have these anywhere
                notes=None,
                active_devices=["None"],
                training_protocol_name=None,
                curriculum_status=None, # TODO - add curriculum stage to behavior training parts
            )
        stimulation_epochs.append(stim_epoch)
    
    if 'optotagging' in nwbfile.processing:
        optogenetic_stimulation = nwbfile.processing['optotagging']['optogenetic_stimulation']
        opto_stim_epoch = StimulusEpoch(
            stimulus_start_time=optogenetic_stimulation['start_time'][0],
            stimulus_end_time=optogenetic_stimulation['stop_time'][-1],
            stimulus_name="Optotagging",
            code=Code(
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
    specimen_id=None, # TODO - confirm not necessary for these file (unless we want to store both donor + specimen id info)
    acquisition_start_time=get_session_start_time(nwbfile, session_info=session_info),
    acquisition_end_time=get_data_stream_end_time(nwbfile),
    protocol_id=None, # TODO - confirm not shared on protocols.io
    ethics_review_id=None,
    instrument_id=get_instrument_id(nwbfile, session_info=session_info),
    acquisition_type=nwbfile.session_description, # TODO - confirm consistent across experiments or if better option
    notes=None,
    coordinate_system=CoordinateSystemLibrary.BREGMA_ARID, # TODO - determine correct coordinate system library
    # calibrations=[],
    # maintenance=[],
    data_streams=[
        DataStream(
            stream_start_time=get_data_stream_start_time(nwbfile),
            stream_end_time=get_data_stream_end_time(nwbfile),
            modalities=get_modalities(nwbfile),
            code=None,
            notes=None,
            active_devices=[ # TODO - determine all active devices names that would apply and their names (e.g. where does eye tracking go?)
                "EPHYS_1",
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
                    volume=get_individual_reward_volume(nwbfile), # TODO - what to do if multiple?
                    volume_unit=VolumeUnit.ML,
                    relative_position=["Anterior"], # TODO - what is the correct information here
                )
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
        reward_consumed_total=get_total_reward_volume(nwbfile), # TODO - check if calculation is sufficient
        reward_consumed_unit=VolumeUnit.ML
    ),
)


if __name__ == "__main__":
    serialized = acquisition.model_dump_json()
    deserialized = Acquisition.model_validate_json(serialized)
    deserialized.write_standard_file(prefix="ephys")