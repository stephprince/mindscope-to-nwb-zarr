"""Generates acquisition metadata from NWB files for visual behavior ephys sessions.

Device names in the configs match the components of the generated Instrument (see
``visual_behavior_ephys/instrument.py``): the per-probe ephys assemblies are named as in
the Visual Coding Neuropixels instrument (reused here), the reward spout is the Visual
Behavior Ophys "Reward Spout", the optotagging laser is the "Optotagging Laser", the
running platform is the "MindScope Running Disc", and visual stimulus epochs list the
"Stimulus Screen" monitor.

Handles both ecephys sessions (probeA-F present, optotagging present) and behavior-only
sessions (no probes, no optotagging): the per-probe configs are simply empty and the
optotagging epoch is omitted.
"""

from datetime import timedelta
from pynwb import NWBFile
import pandas as pd

from aind_data_schema.components.identifiers import Code
from aind_data_schema.core.acquisition import (
    Acquisition,
    StimulusEpoch,
    DataStream,
    AcquisitionSubjectDetails,
)
from aind_data_schema.components.configs import (
    LaserConfig,
    LickSpoutConfig,
    Liquid,
    Valence,
)
from aind_data_schema_models.units import SizeUnit, VolumeUnit, MassUnit
from aind_data_schema_models.stimulus_modality import StimulusModality

from mindscope_to_nwb_zarr.pynwb_utils import (
    get_data_stream_start_time,
    get_data_stream_end_time,
    get_modalities,
)
from mindscope_to_nwb_zarr.aind_data_schema.utils import (
    get_subject_id,
    get_session_start_time,
    get_instrument_id,
    get_total_reward_volume,
    get_individual_reward_volume,
    get_optostimulation_parameters,
    convert_intervals_to_visual_stimulus_epoch,
    EPHYS_GLOBAL_COORDINATE_SYSTEM,
)
# Reuse the Visual Coding Neuropixels per-probe assembly config builder: the Visual
# Behavior Neuropixels NWBs have the identical EcephysProbe probeA-F structure, and the
# config device names it emits ("Ephys Assembly A", "ProbeA", ...) match the instrument
# components reused from that dataset.
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.acquisition import (
    get_ephys_assembly_configs,
)
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.instrument import (
    OPTOTAGGING_LASER_NAME,
    OPTOTAGGING_LASER_WAVELENGTH,
)
from mindscope_to_nwb_zarr.aind_data_schema.visual_behavior_ophys.instrument import (
    REWARD_SPOUT_NAME,
    RUNNING_DISC_NAME,
    STIMULUS_MONITOR_NAME,
)


def get_stimulation_epochs(nwbfile: NWBFile, session_info: pd.Series) -> list[StimulusEpoch]:
    """
    Extract stimulus epochs from NWB file intervals tables.

    The change-detection presentation table is split into active (task) and passive-replay
    epochs. Each visual epoch lists the stimulus monitor ("Stimulus Screen") as its active
    device. When the session has optotagging data (ecephys sessions), a single
    "Optotagging" epoch driven by the 473 nm optotagging laser is appended.

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
            stim_epoch = convert_intervals_to_visual_stimulus_epoch(
                stimulus_name="Change detection - Active",
                table_key=table_key,
                intervals_table=active_intervals,
                nwbfile=nwbfile,
                session_info=session_info,
                active_devices=[STIMULUS_MONITOR_NAME],  # the stimulus monitor in the instrument
            )
            stimulation_epochs.append(stim_epoch)

            passive_intervals = intervals_table.to_dataframe().query('active == False')
            if len(passive_intervals) > 0:
                stim_epoch = convert_intervals_to_visual_stimulus_epoch(
                    stimulus_name="Change detection - Passive replay",
                    table_key=table_key,
                    intervals_table=passive_intervals,
                    nwbfile=nwbfile,
                    session_info=session_info,
                    active_devices=[STIMULUS_MONITOR_NAME],
                )
                stimulation_epochs.append(stim_epoch)
        else:
            # Convert table key to formatted stimulus name
            stimulus_name = table_key.replace('_', ' ').title()
            stim_epoch = convert_intervals_to_visual_stimulus_epoch(
                stimulus_name=stimulus_name,
                table_key=table_key,
                intervals_table=intervals_table.to_dataframe(),
                nwbfile=nwbfile,
                session_info=session_info,
                active_devices=[STIMULUS_MONITOR_NAME],
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
            active_devices=[OPTOTAGGING_LASER_NAME],  # 473 nm optotagging laser in the instrument
            configurations=[
                LaserConfig(
                    device_name=OPTOTAGGING_LASER_NAME,
                    wavelength=OPTOTAGGING_LASER_WAVELENGTH,
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
    # One config per probe, with device names matching the instrument components. Empty for
    # behavior-only sessions (no EcephysProbe devices).
    ephys_assembly_configs = get_ephys_assembly_configs(nwbfile)

    # Devices active on the data stream: each ephys assembly, its probe, and the reward
    # spout (the reward-driven behavior task ran on every session, ephys or behavior-only).
    active_devices = (
        [config.device_name for config in ephys_assembly_configs]
        + [probe.device_name for config in ephys_assembly_configs for probe in config.probes]
        + [REWARD_SPOUT_NAME]
    )

    acquisition = Acquisition(
        subject_id=get_subject_id(nwbfile, session_info=session_info),
        acquisition_start_time=get_session_start_time(nwbfile, session_info=session_info),
        acquisition_end_time=get_data_stream_end_time(nwbfile),
        ethics_review_id=None,  # TODO - obtain if available
        instrument_id=get_instrument_id(nwbfile, session_info=session_info),  # equipment_name; matches the generated Instrument
        acquisition_type=nwbfile.session_description,
        notes=None,
        global_coordinate_system=EPHYS_GLOBAL_COORDINATE_SYSTEM,  # bregma-relative frame the probe transforms resolve into
        data_streams=[
            DataStream(
                stream_start_time=get_data_stream_start_time(nwbfile),
                stream_end_time=get_data_stream_end_time(nwbfile),
                modalities=get_modalities(nwbfile),
                code=None,
                notes=None,
                active_devices=active_devices,
                configurations=[
                    # Per-probe ephys assembly configs (device names match the instrument).
                    *ephys_assembly_configs,
                    # Reward/lick spout; the smallest per-trial reward volume is recorded.
                    LickSpoutConfig(
                        device_name=REWARD_SPOUT_NAME,
                        solution=Liquid.WATER,
                        solution_valence=Valence.POSITIVE,
                        volume=get_individual_reward_volume(nwbfile),
                        volume_unit=VolumeUnit.ML,
                        relative_position=["Anterior"],  # TODO - confirm exact placement
                    ),
                    # The optotagging laser config lives on the "Optotagging" stimulus epoch
                    # (see get_stimulation_epochs), not on the data stream.
                ],
            ),
        ],
        stimulus_epochs=get_stimulation_epochs(nwbfile, session_info),
        subject_details=AcquisitionSubjectDetails(
            animal_weight_prior=None,  # TODO - pull in extra info if available
            animal_weight_post=None,
            weight_unit=MassUnit.G,
            mouse_platform_name=RUNNING_DISC_NAME,  # matches the Disc device in the instrument
            reward_consumed_total=get_total_reward_volume(nwbfile),
            reward_consumed_unit=VolumeUnit.ML,
        ),
    )

    return acquisition
