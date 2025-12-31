"""Generates acquisition metadata for visual behavior ophys behavior-only sessions"""

from datetime import timedelta

import numpy as np
import pandas as pd
from pynwb import NWBFile

from aind_data_schema.components.identifiers import Code
from aind_data_schema.core.acquisition import (
    Acquisition,
    StimulusEpoch,
    DataStream,
    AcquisitionSubjectDetails,
)
from aind_data_schema.components.configs import (
    LickSpoutConfig,
    Liquid,
    Valence,
)
from aind_data_schema.components.coordinates import (
    CoordinateSystemLibrary,
)
from aind_data_schema.components.stimulus import VisualStimulation
from aind_data_schema_models.units import VolumeUnit, MassUnit
from aind_data_schema_models.stimulus_modality import StimulusModality
from aind_data_schema_models.modalities import Modality

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


def get_visual_stimulation(nwbfile: NWBFile, session_info: pd.Series) -> VisualStimulation:
    """Extract visual stimulation information from NWB file"""
    stimulus_parameters = {
        # TODO update for different stages
        # TODO confirm grating_orientations in nwbfile.stimulus_template["grating"].control_description ("gratings_0.0", "gratings_90.0", etc.)
        "grating_orientations": [0.0, 90.0, 180.0, 270.0], 
        "grating_orientation_unit": "degrees",
        "distribution": nwbfile.lab_meta_data["task_parameters"].stimulus_distribution,
        "duration_sec": nwbfile.lab_meta_data["task_parameters"].stimulus_duration_sec,
        "blank_duration_sec": nwbfile.lab_meta_data["task_parameters"].blank_duration_sec,
        "n_stimulus_frames": nwbfile.lab_meta_data["task_parameters"].n_stimulus_frames,
        "response_window_sec": nwbfile.lab_meta_data["task_parameters"].response_window_sec,
        "omitted_flash_fraction": nwbfile.lab_meta_data["task_parameters"].omitted_flash_fraction,
        # TODO Cannot find the below information in the data or whitepaper
        # "grating_spatial_frequencies": [0.02, 0.04, 0.08, 0.16, 0.32],
        # "grating_spatial_frequency_unit": "cycles/degree",
    }
    # Convert any numpy types to native Python types for serialization
    for key, value in stimulus_parameters.items():
        if isinstance(value, (np.integer, np.floating, np.ndarray)):
            stimulus_parameters[key] = value.tolist()

    # Get stimulus template names if available
    # TODO confirm other stimulus types
    stimulus_template_name = []
    if "grating" in nwbfile.stimulus_template:
        stimulus_template_name = nwbfile.stimulus_template["grating"].control_description[:].tolist()

    visual_stimulation = VisualStimulation(
        stimulus_name=session_info["image_set"],  # e.g., "gratings" or "images_A"
        stimulus_parameters=stimulus_parameters,
        stimulus_template_name=stimulus_template_name,
        notes=None,
    )
    return visual_stimulation


def generate_acquisition(nwbfile: NWBFile, session_info: pd.Series) -> Acquisition:
    """
    Generate an Acquisition model from an NWB file and session metadata.

    Parameters
    ----------
    nwbfile : NWBFile
        NWB file containing acquisition data
    session_info : pd.Series
        Session metadata row from the behavior session table

    Returns
    -------
    Acquisition
        AIND Acquisition data model populated with data from the NWB file
    """
    # this script is for behavior only files for the visual behavior ophys experiments
    assert get_modalities(nwbfile) == [Modality.BEHAVIOR]

    visual_stimulation = get_visual_stimulation(nwbfile, session_info)

    acquisition = Acquisition(
        subject_id=get_subject_id(nwbfile, session_info=session_info),
        specimen_id=None,
        acquisition_start_time=get_session_start_time(nwbfile, session_info=session_info),
        acquisition_end_time=get_data_stream_end_time(nwbfile),
        # experimenters=None, # TODO - determine where to extract
        protocol_id=None,
        ethics_review_id=None,  # TODO get from Saskia
        instrument_id=get_instrument_id(nwbfile, session_info=session_info),
        acquisition_type=nwbfile.session_description, # TODO - confirm consistent across experiments or if better option
        notes=None,
        coordinate_system=CoordinateSystemLibrary.BREGMA_ARID, # TODO - determine correct system library
        # instrument and acquisition do not have the same coordinate system. 
        # For Ophys, it will define the location of the imaging FOV in a way that can be entered. Saskia will check.
        # calibrations=None,  # will be difficult to find, so leave out
        # maintenance=None,
        data_streams=[
            DataStream(
                stream_start_time=get_data_stream_start_time(nwbfile),
                stream_end_time=get_data_stream_end_time(nwbfile),
                modalities=get_modalities(nwbfile),
                code=None,
                notes=None,
                active_devices=[  # Instruments need to be defined
                    "Lick_Spout_1",  # placeholder
                ],
                configurations=[
                    # TODO: some sessions have no rewards
                    # LickSpoutConfig(  # Lick spout is specific to the rig
                    #     device_name="Lick_Spout_1",  # placeholder
                    #     solution=Liquid.WATER,
                    #     solution_valence=Valence.POSITIVE,
                    #     volume=get_individual_reward_volume(nwbfile),
                    #     volume_unit=VolumeUnit.ML,
                    #     relative_position=["Anterior"], # TODO - what is the correct information here
                    # )
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
                    nwbfile.intervals['grating_presentations']['start_time'][0] if 'grating_presentations' in nwbfile.intervals else nwbfile.trials['start_time'][0],
                    nwbfile.trials['start_time'][0]
                )),
                stimulus_end_time=nwbfile.session_start_time + timedelta(seconds=max(
                    nwbfile.intervals['grating_presentations']['stop_time'][-1] if 'grating_presentations' in nwbfile.intervals else nwbfile.trials['stop_time'][-1],
                    nwbfile.trials['stop_time'][-1]
                )),
                stimulus_name="Change detection natural images",
                code=Code(
                    url="TODO",  # TODO add URL to stimulus code
                    # TODO add code parameters if available, but it seems like it is not available
                    parameters=visual_stimulation,
                ),
                stimulus_modalities=[StimulusModality.VISUAL],
                performance_metrics=None,
                notes=None,
                active_devices=list(),
                configurations=list(), # TODO - think the options provided do not apply, except maybe labor configurations
                training_protocol_name=session_info["session_type"],  # e.g., "TRAINING_0_gratings_autorewards_15min"
                curriculum_status=get_curriculum_status(session_info)
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
    return acquisition
