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
``active == True`` rows) is emitted as a single epoch. That active epoch records the
session number as its ``curriculum_status`` and the animal's prior-experience descriptors
(experience_level, prior_exposures_to_image_set / _omissions / _session_type) as stimulus
parameters; its ``training_protocol_name`` is left empty because the VBN Procedures define
no training protocol for that field to match. The passive replay and the passive mapping
stimuli (flash / gabor / spontaneous) are split per block and carry no task metadata.

Every VBN visual epoch also drops the redundant ``stimulus_name`` stimulus parameter (the
per-presentation template names are already in ``stimulus_template_name``) and summarizes
the jittery per-presentation ``duration`` column -- a scalar when effectively constant,
otherwise dropped (the raw per-row values remain in the NWB). Both are VBN-only: the other
Mindscope datasets have no ``duration`` column and keep their existing parameter shape.
"""

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
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

# The rig is at the Allen Institute in Seattle. The ecephys_sessions.csv date_of_acquisition
# is the acquisition wall-clock in US/Pacific but is mislabeled "+00:00"; the NWB
# session_start_time is the correct UTC.
_PACIFIC = ZoneInfo("America/Los_Angeles")


def _vbn_acquisition_start_time(nwbfile: NWBFile, session_info: pd.Series) -> datetime:
    """Acquisition start time = the NWB ``session_start_time`` (correct UTC), cross-checked.

    Behavior-only rows: the session table ``date_of_acquisition`` already agrees with the
    NWB (both UTC). Ecephys rows: ``ecephys_sessions.csv`` stores the acquisition wall-clock
    in US/Pacific but labeled ``+00:00``, so reinterpreting that wall-clock as US/Pacific and
    converting to UTC must equal the NWB value -- a DST-aware 7 h (PDT) / 8 h (PST) offset.
    Any other discrepancy raises (fail-loud) rather than silently using the NWB value.
    """
    nwb_start = nwbfile.session_start_time
    csv_dt = datetime.fromisoformat(str(session_info['date_of_acquisition']))
    # Already agrees (behavior-only): use the NWB value.
    if abs((csv_dt.astimezone(timezone.utc) - nwb_start).total_seconds()) < 120:
        return nwb_start
    # Otherwise it must be the Pacific-local-labeled-as-UTC case (ecephys).
    expected_utc = csv_dt.replace(tzinfo=_PACIFIC).astimezone(timezone.utc)
    if abs((expected_utc - nwb_start).total_seconds()) >= 120:
        raise ValueError(
            f"VBN session_start_time mismatch not explained by the US/Pacific offset: "
            f"session table date_of_acquisition={csv_dt.isoformat()}, NWB "
            f"session_start_time={nwb_start.isoformat()}."
        )
    return nwb_start


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


# The change-detection task (the "Change detection - Active" epoch) carries a jittery
# per-presentation ``duration`` column and a redundant ``stimulus_name`` parameter; these
# are collapsed/dropped for every VBN visual epoch. Only VBN's presentation tables have the
# ``duration`` column, so this handling is scoped here rather than in the shared helper.
_VBN_COLLAPSE_OR_DROP_PARAMETERS = {"duration"}
_VBN_DROP_PARAMETERS = {"stimulus_name"}

# Prior-experience descriptors of the change-detection task, recorded as stimulus parameters
# on the active behavior epoch instead of being packed into the curriculum_status string.
# Sourced from ecephys_sessions.csv / behavior_sessions.csv: experience_level is present
# only for ecephys rows and prior_exposures_to_session_type only for behavior rows, so
# whichever are absent from a given row are simply skipped. image_set is intentionally not
# among these -- it is already carried by the presentation table's own image_set column, so
# repeating the (often disagreeing, NaN-for-gratings) session-table image_set here would
# duplicate it.
_TASK_EXPERIENCE_COLUMNS = [
    "experience_level",
    "prior_exposures_to_image_set",
    "prior_exposures_to_omissions",
    "prior_exposures_to_session_type",
]


def _clean_value(value):
    """Coerce a session-table cell to a JSON-friendly scalar: None for missing/NaN, a plain
    Python int for integer-valued numbers (numpy or float), otherwise the value itself."""
    if pd.isna(value):
        return None
    if hasattr(value, "item"):  # numpy scalar -> Python scalar
        value = value.item()
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def _vbn_task_stimulus_parameters(session_info: pd.Series) -> dict:
    """Prior-experience descriptors (experience_level, prior_exposures_*) to merge into the
    active behavior epoch's ``stimulus_parameters`` (see ``_TASK_EXPERIENCE_COLUMNS``)."""
    return {
        column: _clean_value(session_info[column])
        for column in _TASK_EXPERIENCE_COLUMNS
        if column in session_info.index
    }


def _vbn_curriculum_status(session_info: pd.Series) -> str | None:
    """Curriculum status for the active behavior epoch: the session number as a string
    (``curriculum_status`` is an Optional[str]), or None when the session number is absent."""
    if "session_number" not in session_info.index:
        return None
    value = _clean_value(session_info["session_number"])
    return None if value is None else str(value)


def get_stimulation_epochs(nwbfile: NWBFile, session_info: pd.Series) -> list[StimulusEpoch]:
    """
    Extract stimulus epochs from NWB file intervals tables, one per contiguous block.

    A presentation table's ``active == True`` rows are the change-detection *behavior task*;
    they are emitted as a single "Change detection - Active" epoch whose ``curriculum_status``
    is the session number and whose ``stimulus_parameters`` include the animal's
    prior-experience descriptors (from ``session_info``); its ``training_protocol_name`` is
    left empty (no VBN training protocol in Procedures to match). The passive replay
    (``active == False``) and the passive mapping stimuli (tables with no active rows, e.g.
    flash / gabor / spontaneous) are split per ``stimulus_block`` into their own epochs and
    carry no task metadata, mirroring the Visual Coding Neuropixels pipeline. Every visual
    epoch drops the redundant ``stimulus_name`` parameter and summarizes the jittery
    ``duration`` column. A single "Optotagging" epoch driven by the 473 nm laser is appended
    when the session has optotagging data (ecephys sessions).

    Parameters
    ----------
    nwbfile : NWBFile
        NWB file containing intervals tables
    session_info : pd.Series
        Session metadata row (drives curriculum_status and the prior-experience stimulus
        parameters on the active behavior epoch)

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
            # The change-detection behavior task: one epoch for the active block. It carries
            # the session number as curriculum_status and the prior-experience descriptors as
            # stimulus parameters; training_protocol_name is forced empty because the VBN
            # Procedures define no training protocol for the field to match.
            active_df = df[df["active"] == True]  # noqa: E712
            stimulation_epochs.append(
                convert_intervals_to_visual_stimulus_epoch(
                    stimulus_name="Change detection - Active",
                    table_key=table_key,
                    intervals_table=active_df,
                    nwbfile=nwbfile,
                    session_info=None,  # task metadata set explicitly below, not derived from the CSV
                    active_devices=[STIMULUS_MONITOR_NAME],  # the stimulus monitor in the instrument
                    extra_parameters=_vbn_task_stimulus_parameters(session_info),
                    drop_parameters=_VBN_DROP_PARAMETERS,
                    collapse_or_drop_parameters=_VBN_COLLAPSE_OR_DROP_PARAMETERS,
                    training_protocol_name=None,
                    curriculum_status=_vbn_curriculum_status(session_info),
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
                        drop_parameters=_VBN_DROP_PARAMETERS,
                        collapse_or_drop_parameters=_VBN_COLLAPSE_OR_DROP_PARAMETERS,
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
                        drop_parameters=_VBN_DROP_PARAMETERS,
                        collapse_or_drop_parameters=_VBN_COLLAPSE_OR_DROP_PARAMETERS,
                    )
                )

    # Ecephys sessions (EcephysProbe devices present) always ran optotagging; a missing
    # module signals a truncated/incorrect file rather than a behavior-only session.
    has_probes = any(d.__class__.__name__ == "EcephysProbe" for d in nwbfile.devices.values())
    if has_probes and 'optotagging' not in nwbfile.processing:
        raise ValueError(
            "Ecephys session (EcephysProbe devices present) is missing the expected "
            "'optotagging' processing module."
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
        acquisition_start_time=_vbn_acquisition_start_time(nwbfile, session_info),
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
