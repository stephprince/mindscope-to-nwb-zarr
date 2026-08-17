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

Stimulus epochs (see ``get_stimulation_epochs``) follow the Visual Coding Neuropixels
approach of one epoch per contiguous ``stimulus_block``, with one Visual-Behavior-specific
addition: the change-detection *behavior task* block (the presentation table's
``active == True`` rows) is emitted as a single epoch carrying the session's
``training_protocol_name`` and ``curriculum_status``; the passive replay and the passive
mapping stimuli (flash / gabor / spontaneous) are split per block and carry no task
metadata.
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
    get_ethics_review_id,
    get_total_reward_volume,
    get_individual_reward_volume,
    get_reward_volume_notes,
    get_optostimulation_parameters,
    convert_intervals_to_visual_stimulus_epoch,
    EPHYS_GLOBAL_COORDINATE_SYSTEM,
)
# Reuse the Visual Coding Neuropixels per-probe assembly config builder and the per-block
# splitter: the Visual Behavior Neuropixels NWBs have the identical EcephysProbe probeA-F
# structure and the same ``stimulus_block`` column, and the config device names the builder
# emits ("Ephys Assembly A", "ProbeA", ...) match the instrument components reused from
# that dataset.
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.acquisition import (
    get_ephys_assembly_configs,
    _iter_stimulus_blocks,
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


# The entire Visual Behavior Neuropixels cohort is under a single IACUC protocol: every
# subject present in the (frozen) reference/ethics_review_ids.csv -- 65 of the 81 -- maps to
# 1805, so the id is hardcoded here for the whole cohort, including the 16 subjects absent
# from that reference file. When the reference file does list the subject, its value is
# asserted to agree, so a future divergence fails loudly rather than passing silently.
VBN_ETHICS_REVIEW_ID = "1805"


def _vbn_ethics_review_id(subject_id) -> list[str]:
    """Ethics (IACUC) review id for a VBN subject: always ``["1805"]``.

    Asserts that any value the reference CSV provides for the subject matches the hardcoded
    cohort id (so a mismatch, or a future edit to the reference file, fails loudly).
    """
    try:
        looked_up = get_ethics_review_id(subject_id)
    except KeyError:
        looked_up = None
    if looked_up is not None:
        assert looked_up == [VBN_ETHICS_REVIEW_ID], (
            f"ethics_review_ids.csv lists {looked_up} for subject {subject_id}, but the "
            f"Visual Behavior Neuropixels cohort id is [{VBN_ETHICS_REVIEW_ID!r}]."
        )
    return [VBN_ETHICS_REVIEW_ID]


def get_stimulation_epochs(nwbfile: NWBFile, session_info: pd.Series) -> list[StimulusEpoch]:
    """
    Extract stimulus epochs from NWB file intervals tables, one per contiguous block.

    A presentation table's ``active == True`` rows are the change-detection *behavior task*;
    they are emitted as a single "Change detection - Active" epoch that carries the session's
    ``training_protocol_name`` and ``curriculum_status`` (passed via ``session_info``). The
    passive replay (``active == False``) and the passive mapping stimuli (tables with no
    active rows, e.g. flash / gabor / spontaneous) are split per ``stimulus_block`` into
    their own epochs and carry no task metadata (``session_info=None``), mirroring the
    Visual Coding Neuropixels pipeline. A single "Optotagging" epoch driven by the 473 nm
    laser is appended when the session has optotagging data (ecephys sessions).

    Parameters
    ----------
    nwbfile : NWBFile
        NWB file containing intervals tables
    session_info : pd.Series
        Session metadata row (drives training_protocol_name / curriculum_status on the
        active behavior epoch)

    Returns
    -------
    list[StimulusEpoch]
        List of stimulus epochs extracted from the NWB file
    """
    stimulation_epochs = []

    for table_key, intervals_table in nwbfile.intervals.items():
        # skip the generic trials table (behavioral data) and invalid_times sections
        if table_key in ["trials", "invalid_times"]:
            continue

        df = intervals_table.to_dataframe()
        has_active_task = "active" in df.columns and bool((df["active"] == True).any())  # noqa: E712

        if has_active_task:
            # The change-detection behavior task: one epoch for the active block, carrying
            # the session's training protocol + curriculum (via session_info).
            active_df = df[df["active"] == True]  # noqa: E712
            stimulation_epochs.append(
                convert_intervals_to_visual_stimulus_epoch(
                    stimulus_name="Change detection - Active",
                    table_key=table_key,
                    intervals_table=active_df,
                    nwbfile=nwbfile,
                    session_info=session_info,
                    active_devices=[STIMULUS_MONITOR_NAME],  # the stimulus monitor in the instrument
                )
            )
            # The passive replay, split per contiguous block; no task metadata.
            passive_df = df[df["active"] == False]  # noqa: E712
            for _block_id, block_df in _iter_stimulus_blocks(passive_df):
                stimulation_epochs.append(
                    convert_intervals_to_visual_stimulus_epoch(
                        stimulus_name="Change detection - Passive replay",
                        table_key=table_key,
                        intervals_table=block_df,
                        nwbfile=nwbfile,
                        session_info=None,
                        active_devices=[STIMULUS_MONITOR_NAME],
                    )
                )
        else:
            # Passive mapping stimulus (flash / gabor / spontaneous): one epoch per block.
            stimulus_name = table_key.replace("_", " ").title()
            for _block_id, block_df in _iter_stimulus_blocks(df):
                stimulation_epochs.append(
                    convert_intervals_to_visual_stimulus_epoch(
                        stimulus_name=stimulus_name,
                        table_key=table_key,
                        intervals_table=block_df,
                        nwbfile=nwbfile,
                        session_info=None,
                        active_devices=[STIMULUS_MONITOR_NAME],
                    )
                )

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
    subject_id = get_subject_id(nwbfile, session_info=session_info)

    # One config per probe, with device names matching the instrument components. Empty for
    # behavior-only sessions (no EcephysProbe devices).
    ephys_assembly_configs = get_ephys_assembly_configs(nwbfile)

    # Reward/lick spout config. Only emitted when a per-reward volume is available: a
    # no-reward session (get_individual_reward_volume returns None) would otherwise give
    # LickSpoutConfig.volume=None, which fails validation. Mirrors visual_behavior_ophys.
    individual_reward_volume = get_individual_reward_volume(nwbfile)
    reward_configs = []
    reward_active_devices = []
    if individual_reward_volume is not None:
        reward_configs.append(
            LickSpoutConfig(
                device_name=REWARD_SPOUT_NAME,
                solution=Liquid.WATER,
                solution_valence=Valence.POSITIVE,
                volume=individual_reward_volume,  # smallest per-trial reward volume
                volume_unit=VolumeUnit.ML,
                relative_position=["Anterior"],
                notes=get_reward_volume_notes(nwbfile),  # lists all volumes if more than one was used
            )
        )
        reward_active_devices.append(REWARD_SPOUT_NAME)

    # Devices active on the data stream: each ephys assembly, its probe, and the reward
    # spout (when reward was delivered). The optotagging laser is listed on its own epoch.
    active_devices = (
        [config.device_name for config in ephys_assembly_configs]
        + [probe.device_name for config in ephys_assembly_configs for probe in config.probes]
        + reward_active_devices
    )

    acquisition = Acquisition(
        subject_id=subject_id,
        acquisition_start_time=get_session_start_time(nwbfile, session_info=session_info),
        acquisition_end_time=get_data_stream_end_time(nwbfile),
        # Ethics (IACUC) review id: hardcoded cohort id 1805 (asserted against the
        # reference CSV when it lists the subject). See _vbn_ethics_review_id.
        ethics_review_id=_vbn_ethics_review_id(subject_id),
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
                    # Reward/lick spout (present only when reward was delivered).
                    *reward_configs,
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
