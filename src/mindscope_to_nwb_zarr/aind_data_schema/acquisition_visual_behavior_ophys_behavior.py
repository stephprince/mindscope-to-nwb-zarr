"""Generates an example JSON file for visual behavior ephys acquisition"""

from datetime import datetime, timezone, timedelta
import json

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
from aind_data_schema_models.units import TimeUnit, SizeUnit, VolumeUnit, FrequencyUnit, MassUnit
from aind_data_schema_models.brain_atlas import CCFv3
from aind_data_schema_models.stimulus_modality import StimulusModality

import pandas as pd
from pynwb import read_nwb
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
    NumpyJsonEncoder,
)

# example file for initial debugging
# TODO - replace with more general ingestion/generation script
subject_id = 403491
nwbfile_session_id = "20180824T145125"  # example stage 0
nwbfile_session_id = "20180827T141750"  # example stage 1
nwbfile = read_nwb(f"C:/Users/Ryan/Documents/mindscope-to-nwb-zarr/data/sub-{subject_id}_ses-{nwbfile_session_id}_image.nwb")
behavior_session_id = int(nwbfile.identifier)
behavior_session_table = pd.read_csv("C:/Users/Ryan/Documents/mindscope-to-nwb-zarr/data/visual_behavior_ophys_metadata/behavior_session_table.csv")
session_info = behavior_session_table.query("mouse_id == @subject_id and behavior_session_id == @behavior_session_id")
assert len(session_info) == 1, "Expected exactly one matching session info entry"

# this script is for behavior only files for the visual behavior ophys experiments
from aind_data_schema_models.modalities import Modality
assert get_modalities(nwbfile) == [Modality.BEHAVIOR]

breakpoint()

acquisition = Acquisition(
    subject_id=get_subject_id(nwbfile, session_info=session_info),
    specimen_id=None, # TODO - confirm not necessary for these file (unless we want to store both donor + specimen id info)
    acquisition_start_time=get_session_start_time(nwbfile, session_info=session_info),
    acquisition_end_time=get_data_stream_end_time(nwbfile),
    # experimenters=None, # TODO - determine where to extract
    protocol_id=None, # TODO - confirm not shared on protocols.io
    ethics_review_id=None,
    instrument_id=get_instrument_id(nwbfile, session_info=session_info),
    acquisition_type=nwbfile.session_description, # TODO - confirm consistent across experiments or if better option
    notes=None,
    coordinate_system=CoordinateSystemLibrary.BREGMA_ARID, # TODO - determine correct system library
    # calibrations=None,
    # maintenance=None,
    data_streams=[ # TODO - fill in, should behavior + ephys be one data stream or multiple? what is the point of multiple streams?
        DataStream(
            stream_start_time=get_data_stream_start_time(nwbfile),
            stream_end_time=get_data_stream_end_time(nwbfile),
            modalities=get_modalities(nwbfile),
            code=None,
            notes=None,
            active_devices=[
                "Lick_Spout_1",
            ],
            configurations=[
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
    # TODO - handle different stimulus sets for the different training stages
    stimulus_epochs=[    
        StimulusEpoch(
            # in the test file, the trial start time is before the stimulus epoch start time
            # but the trial end time is before the stimulus epoch end time
            # so we pick whichever is earliest for stimulus start time
            # and whichever is latest for stimulus end time
            # TODO: confirm this strategy is OK
            stimulus_start_time=nwbfile.session_start_time + timedelta(seconds=min(
                nwbfile.intervals['grating_presentations']['start_time'][0], 
                nwbfile.trials['start_time'][0]
            )),
            stimulus_end_time=nwbfile.session_start_time + timedelta(seconds=max(
                nwbfile.intervals['grating_presentations']['stop_time'][-1], 
                nwbfile.trials['stop_time'][-1]
            )),
            stimulus_name="Change detection natural images",
            code=Code(
                url="TODO",  # TODO - add URL to stimulus code
                name=None,
                version=None,
                container=None,
                run_script=None,
                language=None,
                language_version=None,
                input_data=None,
                core_dependency=Software(
                    name="TODO",
                    version=None,
                ), # TODO - add software if available
                parameters=VisualStimulation(
                    stimulus_name=session_info["image_set"].values[0], # e.g., "gratings" or "images_A"
                    stimulus_parameters={
                        # TODO update for different stages
                        "grating_orientations": [0.0, 90.0, 180.0, 270.0],  # TODO confirm in nwbfile.stimulus_template["grating"].control_description ("gratings_0.0", "gratings_90.0", etc.)
                        "grating_orientation_unit": "degrees",
                        # "distribution": nwbfile.lab_meta_data["task_parameters"].stimulus_distribution,
                        # "duration_sec": nwbfile.lab_meta_data["task_parameters"].stimulus_duration_sec,
                        # "blank_duration_sec": nwbfile.lab_meta_data["task_parameters"].blank_duration_sec,
                        # "n_stimulus_frames": nwbfile.lab_meta_data["task_parameters"].n_stimulus_frames,
                        # "response_window_sec": nwbfile.lab_meta_data["task_parameters"].response_window_sec,
                        # "omitted_flash_fraction": nwbfile.lab_meta_data["task_parameters"].omitted_flash_fraction,
                        # Cannot find the below information
                        # "grating_spatial_frequencies": [0.02, 0.04, 0.08, 0.16, 0.32],
                        # "grating_spatial_frequency_unit": "cycles/degree",
                    },
                    stimulus_template_name=nwbfile.stimulus_template["grating"].control_description[:].tolist(),
                    notes=None,
                ),
            ),
            stimulus_modalities=[StimulusModality.VISUAL],
            performance_metrics=None,
            notes=None,
            active_devices=list(),
            configurations=list(), # TODO - think the options provided do not apply, except maybe labor configurations
            training_protocol_name=session_info["session_type"].values[0],  # e.g., "TRAINING_0_gratings_autorewards_15min"
            curriculum_status=json.dumps(dict(
                behavior_type=session_info["behavior_type"].values[0],  # e.g., "active_behavior"
                experience_level=session_info["experience_level"].values[0],  # e.g., "Training"
                prior_exposures_to_image_set=session_info["prior_exposures_to_image_set"].values[0],  # e.g., nan
                prior_exposures_to_omissions=session_info["prior_exposures_to_omissions"].values[0],  # e.g., 0
                prior_exposures_to_session_type=session_info["prior_exposures_to_session_type"].values[0],  # e.g., 0
            ), cls=NumpyJsonEncoder)
        ),
    ], 
    # manipulations=None, # TODO - think this is None (seems to be injections)
    subject_details=AcquisitionSubjectDetails(
        animal_weight_prior=None,
        animal_weight_post=None,
        weight_unit=MassUnit.G,
        anaesthesia=None,
        mouse_platform_name="Running Wheel", # TODO - determine where to extract if needed
        reward_consumed_total=get_total_reward_volume(nwbfile), # TODO - check if calculation is sufficient
        reward_consumed_unit=VolumeUnit.ML
    ),
)


if __name__ == "__main__":
    serialized = acquisition.model_dump_json()
    deserialized = Acquisition.model_validate_json(serialized)
    deserialized.write_standard_file(prefix="ophys_visual_behavior")