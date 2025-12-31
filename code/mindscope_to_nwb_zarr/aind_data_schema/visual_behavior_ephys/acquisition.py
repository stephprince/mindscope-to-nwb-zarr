"""Generates acquisition metadata from NWB files for visual behavior ephys sessions"""

from datetime import timedelta
from pynwb import NWBFile

from aind_data_schema.components.identifiers import Code
from aind_data_schema.core.acquisition import (
    Acquisition,
    StimulusEpoch,
    DataStream,
    AcquisitionSubjectDetails,
)
from aind_data_schema.components.configs import (
    ManipulatorConfig,
    EphysAssemblyConfig,
    LaserConfig,
    LickSpoutConfig,
    Liquid,
    Valence,
)
from aind_data_schema.components.coordinates import Translation, CoordinateSystemLibrary
from aind_data_schema_models.units import SizeUnit, VolumeUnit, MassUnit
from aind_data_schema_models.stimulus_modality import StimulusModality
import pandas as pd

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
    get_probe_configs,
    get_optostimulation_parameters,
    convert_intervals_to_stimulus_epochs,
)


def get_stimulation_epochs(nwbfile: NWBFile, session_info: pd.Series) -> list[StimulusEpoch]:
    """
    Extract stimulus epochs from NWB file intervals tables.

    Parameters
    ----------
    nwbfile : NWBFile
        NWB file containing intervals tables
    session_info : pd.Series
        Session metadata row

    Returns
    -------
    list[StimulusEpoch]
        List of stimulus epochs extracted from the NWB file
    """
    stimulation_epochs = []

    for table_key, intervals_table in nwbfile.intervals.items():
        # skip generic trials table that contains behavioral data and invalid_times sections
        if table_key in ["trials", "invalid_times"]:
            continue
        # split active and passive behavior sessions into different stimulus epochs
        elif table_key == "Natural_Images_Lum_Matched_set_ophys_G_2019_presentations":
            active_intervals = intervals_table.to_dataframe().query('active == True')
            stimulus_name = "Change detection - Active"
            stim_epoch = convert_intervals_to_stimulus_epochs(
                stimulus_name=stimulus_name,
                table_key=table_key,
                intervals_table=active_intervals,
                nwbfile=nwbfile,
                session_info=session_info,
            )
            stimulation_epochs.append(stim_epoch)

            passive_intervals = intervals_table.to_dataframe().query('active == False')
            if len(passive_intervals) > 0:
                stimulus_name = "Change detection - Passive replay"
                stim_epoch = convert_intervals_to_stimulus_epochs(
                    stimulus_name=stimulus_name,
                    table_key=table_key,
                    intervals_table=passive_intervals,
                    nwbfile=nwbfile,
                    session_info=session_info,
                )
                stimulation_epochs.append(stim_epoch)
        else:
            # Convert table key to formatted stimulus name
            stimulus_name = table_key.replace('_', ' ').title()
            stim_epoch = convert_intervals_to_stimulus_epochs(
                stimulus_name=stimulus_name,
                table_key=table_key,
                intervals_table=intervals_table.to_dataframe(),
                nwbfile=nwbfile,
                session_info=session_info,
            )
            stimulation_epochs.append(stim_epoch)

    if 'optotagging' in nwbfile.processing:
        optogenetic_stimulation = nwbfile.processing['optotagging']['optogenetic_stimulation']
        opto_stim_epoch = StimulusEpoch(
            stimulus_start_time=timedelta(seconds=optogenetic_stimulation['start_time'][0]) + nwbfile.session_start_time,
            stimulus_end_time=timedelta(seconds=optogenetic_stimulation['stop_time'][-1]) + nwbfile.session_start_time,
            stimulus_name="Optotagging",
            code=Code(  # TODO - add code source if available
                url="None",
                parameters=get_optostimulation_parameters(optogenetic_stimulation),
            ),
            stimulus_modalities=[StimulusModality.OPTOGENETICS],
            performance_metrics=None,
            notes=None,
            active_devices=["Laser_1"],
            configurations=[
                LaserConfig(
                    device_name="Laser_1",
                    wavelength=473,  # from technical whitepaper
                    wavelength_unit=SizeUnit.NM,
                ),
            ],
            training_protocol_name=None,
            curriculum_status=None,
        )
        stimulation_epochs.append(opto_stim_epoch)

    return stimulation_epochs


def generate_acquisition(nwbfile: NWBFile, session_info: pd.Series) -> Acquisition:
    """
    Generate an Acquisition model from an NWB file and session metadata.

    Parameters
    ----------
    nwbfile : NWBFile
        NWB file containing acquisition data
    session_info : pd.Series
        Session metadata row

    Returns
    -------
    Acquisition
        AIND Acquisition data model populated with data from the NWB file
    """
    acquisition = Acquisition(
        subject_id=get_subject_id(nwbfile, session_info=session_info),
        acquisition_start_time=get_session_start_time(nwbfile, session_info=session_info),
        acquisition_end_time=get_data_stream_end_time(nwbfile),
        ethics_review_id=None,  # TODO - obtain if available - YES, @Saskia
        instrument_id=get_instrument_id(nwbfile, session_info=session_info),
        acquisition_type=nwbfile.session_description,
        notes=None,
        coordinate_system=CoordinateSystemLibrary.BREGMA_ARID,  # TODO - determine correct coordinate system library, will also be defined with instrument (not required to be same as acquisition)
        # coordinate system info might not be available, will check @Saskia
        # calibrations=[],  # TODO - add if available - will be difficult to find, probably not
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
                            device_name="Manipulator_1",  # TODO - fill in with correct information
                            coordinate_system=CoordinateSystemLibrary.MPM_MANIP_RFB,  # should be standardized (confirm relative to bregma, positions) @Saskia
                            local_axis_positions=Translation(translation=[0, 0, 0],),  # TODO - fill in with correct positions @Saskia
                        ),
                        probes=get_probe_configs(nwbfile),
                    ),
                    LaserConfig(  # TODO - should this go here or in the stimulation epochs configuration field? currently in both
                        device_name="Laser_1",  # placeholder
                        wavelength=473,  # from technical whitepaper
                        wavelength_unit=SizeUnit.NM,
                    ),
                    LickSpoutConfig(
                        device_name="Lick_Spout_1",  # placeholder
                        solution=Liquid.WATER,
                        solution_valence=Valence.POSITIVE,
                        volume=get_individual_reward_volume(nwbfile),
                        volume_unit=VolumeUnit.ML,
                        relative_position=["Anterior"],  # TODO - what is the correct information here?
                    ),
                    # TODO - add information about Monitor, Camera, LED from nwbfile.processing['eye_tracking_rig_metadata']['eye_tracking_rig_metadata']
                    # TODO - should we add MousePlatformConfig here too?
                ],
            ),
        ],
        stimulus_epochs=get_stimulation_epochs(nwbfile, session_info),
        subject_details=AcquisitionSubjectDetails(
            animal_weight_prior=None,  # TODO - pull in extra info if available - likely not available @Saskia
            animal_weight_post=None,
            weight_unit=MassUnit.G,
            mouse_platform_name="Running Wheel",
            reward_consumed_total=get_total_reward_volume(nwbfile),
            reward_consumed_unit=VolumeUnit.ML,
        ),
    )

    return acquisition
