"""Generates an example JSON file for visual behavior ephys acquisition"""

from datetime import datetime, timezone


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
from aind_data_schema.components.stimulus import VisualStimulation, OptoStimulation
from aind_data_schema_models.units import TimeUnit, SizeUnit, VolumeUnit
from aind_data_schema_models.brain_atlas import CCFv3
from aind_data_schema_models.stimulus_modality import StimulusModality

import pandas as pd
from pynwb import read_nwb
from pathlib import Path
from mindscope_to_nwb_zarr.pynwb_utils import get_acquisition_end_time, get_modalities
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
probe_table = pd.read_csv(cache_dir / "probes.csv")
session_info = ephys_session_table.query("mouse_id == @subject_id and ecephys_session_id == @session_id")
if len(session_info) == 0:
    raise ValueError(f"No session info found for subject_id={subject_id}, session_id={session_id}")
probe_info = probe_table.query("ecephys_session_id == @session_id")

def get_probe_configs(nwbfile, probe_info):
    probe_configs = []
    for device in nwbfile.devices.values():
        if device.__class__.__name__ == "EcephysProbe":
            # TODO - this may miss some targeted structures if not an exact string match
            locations = (nwbfile.electrodes.to_dataframe()
                         .query('group_name == @device.name')['location'].unique().tolist())
            targeted_structures = [getattr(CCFv3, l) for l in locations if getattr(CCFv3, l, None) is not None]
            
            probe_configs.append(
                ProbeConfig(
                    device_name=device.name,
                    primary_targeted_structure=CCFv3.VIS, # TODO - update if need to be more specific
                    other_targeted_structure=targeted_structures,
                    atlas_coordinate=AtlasCoordinate(
                        coordinate_system=AtlasLibrary.CCFv3_10um,
                        translation=[8150, 3250, 7800], # TODO - should be target region coordinate
                    ),
                    coordinate_system=CoordinateSystemLibrary.MPM_MANIP_RFB, # TODO - what should this be?
                    transform=[Translation(translation=[5000, 5000, 0, 1],),], # TODO - what should this be?
                    notes=None,
                )
            )
    
    return probe_configs


acquisition = Acquisition(
    subject_id=get_subject_id(nwbfile, session_info=session_info),
    specimen_id=None, # TODO - confirm not necessary for these file (unless we want to store both donor + specimen id info)
    acquisition_start_time=get_session_start_time(nwbfile, session_info=session_info),
    acquisition_end_time=get_acquisition_end_time(nwbfile),
    protocol_id=None, # TODO - confirm not shared on protocols.io
    ethics_review_id=None,
    instrument_id=get_instrument_id(nwbfile, session_info=session_info),
    acquisition_type=nwbfile.session_description, # TODO - confirm consistent across experiments or if better option
    notes=None,
    coordinate_system=CoordinateSystemLibrary.BREGMA_ARID, # TODO - determine correct system library
    calibrations=None,
    maintenance=None,
    data_streams=[ # TODO - fill in, should behavior + ephys be one data stream or multiple? what is the point of multiple streams?
        DataStream(
            stream_start_time=None, # TODO - fill in from timeseries (first value of group)
            stream_end_time=datetime(year=2023, month=4, day=25, hour=3, minute=16, second=0, tzinfo=timezone.utc),
            modalities=get_modalities(nwbfile),
            code=None,
            notes=None,
            active_devices=[ # TODO - determine active devices names that would apply, any other ones
                "EPHYS_1",
                "Laser_473nm",
                "Lick_Spout_1",
            ],
            configurations=[ # TODO - determine which configurations apply to us
                EphysAssemblyConfig(
                    device_name="EPHYS_1",
                    manipulator=ManipulatorConfig(
                        device_name="None", # TODO - fill in
                        coordinate_system=CoordinateSystemLibrary.MPM_MANIP_RFB,
                        local_axis_positions=Translation(translation=[0, 0, 0],), # TODO - fill in with correct positions
                    ),
                    probes=get_probe_configs(nwbfile, probe_info),
                ),
                LaserConfig(
                    device_name="Laser_473nm", # TODO - determine correct laser name
                    wavelength=473, # from technical whitepaper
                    wavelength_unit=SizeUnit.NM,
                ),
                LickSpoutConfig(
                    device_name="Lick_Spout_1", # TODO - determine correct lick spout name
                    solution=Liquid.WATER,
                    solution_valence=Valence.POSITIVE, # TODO - is this correct
                    volume=get_individual_reward_volume(nwbfile), # TODO - what to do if multiple?
                    volume_unit=VolumeUnit.UL,
                    relative_position=["Anterior"], # TODO - fill in if available
                )
            ],
         ),
    ],
    stimulus_epochs=[    
        StimulusEpoch(
            stimulus_start_time=nwbfile.intervals['Natural_Images_Lum_Matched_set_ophys_G_2019_presentations']['start_time'][0],
            stimulus_end_time=nwbfile.intervals['Natural_Images_Lum_Matched_set_ophys_G_2019_presentations']['end_time'][-1],
            stimulus_name="Visual change detection task",
            code=Code( # TODO - acquire additional info about the code used for this task
                url=None,
                name=None,
                version=None,
                container=None,
                run_script=None,
                language=None,
                language_version=None,
                input_data=None,
                core_dependency=Software(
                    name=None,
                    version=None,), # TODO - add software if available
                parameters=VisualStimulation(
                    stimulus_name="'Natural_Images_Lum_Matched_set_ophys_G_2019_presentations'",
                    # stimulus_parameters={
                    #     "grating_orientations": [0, 45, 90, 135],
                    #     "grating_orientation_unit": "degrees",
                    #     "grating_spatial_frequencies": [0.02, 0.04, 0.08, 0.16, 0.32],
                    #     "grating_spatial_frequency_unit": "cycles/degree",
                    # },
                    stimulus_template_name=None,
                    notes=None,
                ),
            ),
            stimulus_modalities=[StimulusModality.VISUAL],
            performance_metrics=None,
            notes=None,
            active_devices=None,
            configurations=None, # TODO - think the options provided do not apply, except maybe labor configurations
            training_protocol_name=None,
            curriculum_status=None,
        ),
        StimulusEpoch(
            stimulus_start_time=nwbfile.intervals['flash_250ms_presentations']['start_time'][0],
            stimulus_end_time=nwbfile.intervals['flash_250ms_presentations']['end_time'][-1],
            stimulus_name="Full-field flashes",
            code=Code(
                url=None,
                name=None,
                version=None,
                container=None,
                run_script=None,
                language=None,
                language_version=None,
                input_data=None,
                core_dependency=Software(
                    name=None,
                    version=None,), # TODO - add software if available
                parameters=VisualStimulation(
                    stimulus_name="flash_250ms_presentations",
                    stimulus_parameters={},
                    stimulus_template_name=None,
                    notes=None,
                ),
            ),
            stimulus_modalities=[StimulusModality.VISUAL],
            performance_metrics=None,
            notes=None,
            active_devices=None,
            configurations=None, # TODO - think the options provided do not apply, except maybe labor configurations
            training_protocol_name=None,
            curriculum_status=None,
        ),
        StimulusEpoch(
            stimulus_start_time=nwbfile.intervals['gabor_20_deg_250ms_presentations']['start_time'][0],
            stimulus_end_time=nwbfile.intervals['gabor_20_deg_250ms_presentations']['end_time'][-1],
            stimulus_name="Gabors",
            code=Code(
                url=None,
                name=None,
                version=None,
                container=None,
                run_script=None,
                language=None,
                language_version=None,
                input_data=None,
                core_dependency=Software(
                    name=None,
                    version=None,), # TODO - add software if available
                parameters=VisualStimulation(
                    stimulus_name="Gabor 20 deg 250 ms presentations",
                    stimulus_parameters={},
                    stimulus_template_name=None,
                    notes=None,
                ),
            ),
            stimulus_modalities=[StimulusModality.VISUAL],
            performance_metrics=None,
            notes=None,
            active_devices=None,
            configurations=None, # TODO - think the options provided do not apply, except maybe labor configurations
            training_protocol_name=None,
            curriculum_status=None,
        ),
        StimulusEpoch(
            stimulus_start_time=nwbfile.processing['optotagging']['optogenetic_stimulation']['start_time'][0],
            stimulus_end_time=nwbfile.processing['optotagging']['optogenetic_stimulation']['end_time'][-1],
            stimulus_name="Optotagging",
            code=Code(
                url=None,
                name=None,
                version=None,
                container=None,
                run_script=None,
                language=None,
                language_version=None,
                input_data=None,
                core_dependency=Software(
                    name=None,
                    version=None,), # TODO - add software if available
                parameters={
                    OptoStimulation( # TODO - add multiple types of opto stimulation?
                        stimulus_name="Pulse",
                        pulse_shape=(nwbfile
                                    .processing['optotagging']['optogenetic_stimulation']
                                    .to_dataframe()
                                    .query('stimulus_name == "pulse"')['condition']
                                    .values[0]),
                        pulse_frequency=None, # TODO - calculate
                        pulse_frequency_unit=TimeUnit.SECONDS,
                        number_pulse_trains=None, # TODO - calculate
                        pulse_width=(nwbfile
                                    .processing['optotagging']['optogenetic_stimulation']
                                    .to_dataframe()
                                    .query('stimulus_name == "pulse"'))['duration'],
                        pulse_width_unit=TimeUnit.SECONDS,
                        baseline_duration=None, # TODO - calculate and add if needed
                        baseline_duration_unit=None,
                        other_parameters=None,
                        notes=None,
                    ),
                    OptoStimulation(
                        stimulus_name="Raised cosine",
                        pulse_shape=(nwbfile
                                    .processing['optotagging']['optogenetic_stimulation']
                                    .to_dataframe()
                                    .query('stimulus_name == "raised_cosine"')['condition']
                                    .values[0]),
                        pulse_frequency=None, # TODO - calculate
                        pulse_frequency_unit=TimeUnit.SECONDS,
                        number_pulse_trains=None, # TODO - calculate
                        pulse_width=(nwbfile
                                    .processing['optotagging']['optogenetic_stimulation']
                                    .to_dataframe()
                                    .query('stimulus_name == "raised_cosine"'))['duration'], # convert to mss
                        pulse_width_unit=TimeUnit.SECONDS,
                        baseline_duration=None, # TODO - calculate and add if needed
                        baseline_duration_unit=None,
                        other_parameters=None,
                        notes=None,
                    ),
                },
            ),
            stimulus_modalities=[StimulusModality.OPTOGENETICS],
            performance_metrics=None,
            notes=None,
            active_devices=None,
            configurations=None, # TODO - think the options provided do not apply, except maybe labor configurations
            training_protocol_name=None,
            curriculum_status=None,
        ),
    ], 
    manipulations=None, # TODO - think this is None (seems to be injections)
    subject_details=AcquisitionSubjectDetails(
        animal_weight_prior=None,
        animal_weight_post=None,
        weight_unit="grams",
        anaesthesia=None,
        mouse_platform_name="running wheel", # TODO - determine where to extract if needed
        reward_consumed_total=get_total_reward_volume(nwbfile), # TODO - check if calculation is sufficient
    ),
)


if __name__ == "__main__":
    serialized = acquisition.model_dump_json()
    deserialized = Acquisition.model_validate_json(serialized)
    deserialized.write_standard_file(prefix="ephys")