"""Generates acquisition metadata from NWB files for visual coding ephys sessions"""

import warnings
import pandas as pd

from datetime import datetime, timedelta
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
    LightEmittingDiodeConfig,
)
from aind_data_schema.components.coordinates import Translation, CoordinateSystemLibrary
from aind_data_schema_models.units import SizeUnit
from aind_data_schema_models.stimulus_modality import StimulusModality

from mindscope_to_nwb_zarr.pynwb_utils import (
    get_data_stream_start_time,
    get_data_stream_end_time,
    get_modalities
)
from mindscope_to_nwb_zarr.aind_data_schema.utils import (
    build_probe_config,
    probe_letter_from_device_name,
    get_optostimulation_parameters,
    get_ethics_review_id,
    convert_intervals_to_visual_stimulus_epoch,
    EPHYS_GLOBAL_COORDINATE_SYSTEM,
)
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.instrument import (
    ephys_assembly_name,
    manipulator_name,
    probe_name,
    uses_optotagging_laser,
    optotagging_device_name,
    get_experiment_metadata,
    get_acquisition_start_time,
    get_rig_id,
    get_mouse_id,
    ACQUISITION_TIMEZONE,
    OPTOTAGGING_LASER_WAVELENGTH,
)


# Tolerance for cross-checking the computed acquisition end time (the last data
# timestamp) against the CSV ExperimentCompleteTime. The last data sample can trail
# the platform's recorded completion by a little; a larger gap indicates a bad record
# (e.g. session 840012044, whose CSV complete time is only ~2 min after its start).
ACQUISITION_END_TIME_TOLERANCE = timedelta(minutes=10)


def check_acquisition_end_time(acquisition_end_time: datetime, experiment_metadata: pd.Series,
                               session_id: int) -> None:
    """Warn if the computed acquisition end time deviates from the CSV ExperimentCompleteTime.

    ``acquisition_end_time`` is derived from the last data timestamp; ``ExperimentCompleteTime``
    is the platform-recorded session end. They should agree to within
    ``ACQUISITION_END_TIME_TOLERANCE``; a larger deviation flags a suspect CSV row or file.
    """
    if acquisition_end_time is None:
        return
    complete_time = datetime.fromisoformat(
        experiment_metadata['ExperimentCompleteTime']
    ).replace(tzinfo=ACQUISITION_TIMEZONE)
    deviation = abs(acquisition_end_time - complete_time)
    if deviation > ACQUISITION_END_TIME_TOLERANCE:
        warnings.warn(
            f"Session {session_id}: computed acquisition_end_time "
            f"{acquisition_end_time.isoformat()} deviates from CSV ExperimentCompleteTime "
            f"{complete_time.isoformat()} by {deviation} (> {ACQUISITION_END_TIME_TOLERANCE})."
        )


def get_optotagging_config(session_id: int) -> LaserConfig | LightEmittingDiodeConfig:
    """Build the config for the optotagging light source a session used.

    Sessions with id >= 789848216 used a 473 nm laser; earlier sessions used a
    465 nm LED. ``LightEmittingDiodeConfig`` has no wavelength field (the
    wavelength is recorded on the instrument device instead), whereas
    ``LaserConfig`` carries it.
    """
    device_name = optotagging_device_name(session_id)
    if uses_optotagging_laser(session_id):
        return LaserConfig(
            device_name=device_name,
            wavelength=OPTOTAGGING_LASER_WAVELENGTH,
            wavelength_unit=SizeUnit.NM,
        )
    return LightEmittingDiodeConfig(device_name=device_name)


def _warn_on_interleaved_stimulus_epochs(stimulus_epochs: list[StimulusEpoch], session_id: int) -> None:
    """Warn if any visual stimulus epochs overlap in time.

    Each visual stimulus epoch spans its intervals table from the first presentation to
    the last (see ``get_stimulation_epochs``). When a stimulus recurs in non-contiguous
    blocks (interleaved with other stimuli), that whole-table span overlaps other
    stimuli's spans. The epochs are not yet split into per-block epochs, so this only
    warns; the overlap can be addressed in a future run if it proves to matter.
    """
    spans = sorted(
        ((e.stimulus_start_time, e.stimulus_end_time, e.stimulus_name) for e in stimulus_epochs),
        key=lambda span: span[0],
    )
    interleaved = []
    running_end = None
    for start, end, name in spans:
        if running_end is not None and start < running_end:
            interleaved.append(name)
        if running_end is None or end > running_end:
            running_end = end
    if interleaved:
        warnings.warn(
            f"Session {session_id}: stimulus epochs interleave in time "
            f"({', '.join(interleaved)}). Each stimulus intervals table is emitted as one "
            f"epoch spanning its full extent, so non-contiguous blocks overlap other "
            f"stimuli. Epochs are not yet split into per-block epochs."
        )


def get_stimulation_epochs(nwbfile: NWBFile, session_info: pd.Series,
                           session_start_time: datetime = None) -> list[StimulusEpoch]:
    """
    Extract stimulus epochs from NWB file intervals tables.

    Parameters
    ----------
    nwbfile : NWBFile
        NWB file containing intervals tables
    session_info : pd.Series
        Session metadata row
    session_start_time : datetime, optional
        Absolute time to anchor interval offsets to. The NWB ``session_start_time``
        for these files is a packaging date, so callers pass the real acquisition
        start (from the reference CSV) here. Defaults to ``nwbfile.session_start_time``.

    Returns
    -------
    list[StimulusEpoch]
        List of stimulus epochs extracted from the NWB file
    """
    if session_start_time is None:
        session_start_time = nwbfile.session_start_time

    stimulation_epochs = []

    for table_key, intervals_table in nwbfile.intervals.items():
        # skip generic trials table that contains behavioral data and invalid_times sections
        if table_key in ["trials", "invalid_times"]:
            continue

        # Convert table key to formatted stimulus name
        stimulus_name = table_key.replace('_', ' ').title()

        intervals_table_filtered = intervals_table.to_dataframe()
        stim_epoch = convert_intervals_to_visual_stimulus_epoch(
            stimulus_name=stimulus_name,
            table_key=table_key,
            intervals_table=intervals_table_filtered,
            nwbfile=nwbfile,
            session_info=session_info,
            session_start_time=session_start_time,
        )
        stimulation_epochs.append(stim_epoch)

    # Warn if these whole-table visual epochs overlap (interleaved stimulus blocks).
    _warn_on_interleaved_stimulus_epochs(stimulation_epochs, int(session_info['id']))

    if 'optotagging' in nwbfile.processing:
        optogenetic_stimulation = nwbfile.processing['optotagging']['optogenetic_stimulation']
        session_id = int(session_info['id'])
        opto_stim_epoch = StimulusEpoch(
            stimulus_start_time=timedelta(seconds=optogenetic_stimulation['start_time'][0]) + session_start_time,
            stimulus_end_time=timedelta(seconds=optogenetic_stimulation['stop_time'][-1]) + session_start_time,
            stimulus_name="Optotagging",
            code=Code(  # TODO - add code source if available
                url="None",
                parameters=get_optostimulation_parameters(optogenetic_stimulation),
            ),
            stimulus_modalities=[StimulusModality.OPTOGENETICS],
            performance_metrics=None,
            notes=None,
            # Sessions with id >= 789848216 used a 473 nm laser; earlier ones a 465 nm LED.
            active_devices=[optotagging_device_name(session_id)],
            configurations=[get_optotagging_config(session_id)],
            training_protocol_name=None,
            curriculum_status=None,
        )
        stimulation_epochs.append(opto_stim_epoch)

    return stimulation_epochs


def get_ephys_assembly_configs(nwbfile: NWBFile) -> list[EphysAssemblyConfig]:
    """Build one EphysAssemblyConfig per probe present in the session.

    Each Neuropixels probe sits in its own ephys assembly (with its own
    manipulator) in the instrument. The probe device in the NWB file is named
    ``probeA`` .. ``probeF``; the corresponding instrument component names
    (``Ephys Assembly {letter}``, ``Ephys Assembly {letter} Manipulator``,
    ``Probe{letter}``) are derived from the same letter so the configs link to
    the instrument. Sessions with fewer than six probes simply yield fewer
    configs.
    """
    assembly_configs = []
    for device in nwbfile.devices.values():
        if device.__class__.__name__ == "EcephysProbe":
            letter = probe_letter_from_device_name(device.name)
            assembly_configs.append(
                EphysAssemblyConfig(
                    device_name=ephys_assembly_name(letter),
                    manipulator=ManipulatorConfig(
                        device_name=manipulator_name(letter),
                        # From Saskia: Since the probe config coordinates that Josh provided
                        # have the full transform in the global space, the manipulator
                        # coordinates are largely meaningless.
                        coordinate_system=CoordinateSystemLibrary.MPM_MANIP_RFB,
                        local_axis_positions=Translation(translation=[0, 0, 0]),
                    ),
                    probes=[build_probe_config(nwbfile, device, device_name=probe_name(letter))],
                )
            )
    return assembly_configs


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
    # One config per probe, with device names matching the instrument components.
    ephys_assembly_configs = get_ephys_assembly_configs(nwbfile)
    session_id = int(session_info['id'])

    # Experiment start time, rig, and operator come from the reference CSV (the
    # original platform JSON files). Session 819701982 is absent from the CSV, so
    # fall back to the NWB session start time and no operator.
    experiment_metadata = get_experiment_metadata(session_id)
    # Rig id ("NP.1"/"NP.2") from the same source as the instrument file, so the
    # acquisition instrument_id matches the generated Instrument.
    instrument_id = get_rig_id(session_id)
    # Ethics (IACUC) review id, keyed by the 6-digit mouse id from the subject mapping
    # (via the NWB subject_id, present for all 58 sessions incl. 819701982).
    ethics_review_id = get_ethics_review_id(get_mouse_id(nwbfile))
    # Corrected acquisition start (CSV ExperimentStartTime; NWB fallback for 819701982).
    acquisition_start_time = get_acquisition_start_time(nwbfile, session_info)
    experimenters = [experiment_metadata['operatorID']] if experiment_metadata is not None else []

    # End/stream/epoch times are NWB offsets re-anchored to the corrected start, since
    # the file's session_start_time is a packaging date (see acquisition_start_time).
    acquisition_end_time = get_data_stream_end_time(nwbfile, session_start_time=acquisition_start_time)
    # Cross-check the computed end against the CSV's independently-recorded completion time.
    if experiment_metadata is not None:
        check_acquisition_end_time(acquisition_end_time, experiment_metadata, session_id)

    # The optotagging light source used this session: a 473 nm laser for sessions
    # with id >= 789848216, otherwise a 465 nm LED.
    optotagging_name = optotagging_device_name(session_id)
    # active_devices lists the instrument components active this session: each
    # ephys assembly, its probe, and the optotagging light source.
    active_devices = (
        [config.device_name for config in ephys_assembly_configs]
        + [probe.device_name for config in ephys_assembly_configs for probe in config.probes]
        + [optotagging_name]
    )

    acquisition = Acquisition(
        subject_id=get_mouse_id(nwbfile),  # 6-digit mouse ID (the NWB subject_id is a 9-digit LIMS id)
        acquisition_start_time=acquisition_start_time,  # from reference CSV (platform JSON)
        acquisition_end_time=acquisition_end_time,  # NWB offset re-anchored to the corrected start
        experimenters=experimenters,  # operatorID from reference CSV
        ethics_review_id=ethics_review_id,  # IACUC review id, looked up by mouse id (all 58 sessions)
        instrument_id=instrument_id,  # actual rig ("NP.1"/"NP.2"); matches the generated Instrument
        acquisition_type=nwbfile.stimulus_notes,  # e.g. "brain_observatory_1.1" / "functional_connectivity"; asserted == session_info['session_type'] upstream
        notes=None,
        coordinate_system=EPHYS_GLOBAL_COORDINATE_SYSTEM,  # bregma-relative frame the probe transforms resolve into
        # calibrations=[],
        # maintenance=[],
        data_streams=[
            DataStream(
                stream_start_time=get_data_stream_start_time(nwbfile, session_start_time=acquisition_start_time),
                stream_end_time=get_data_stream_end_time(nwbfile, session_start_time=acquisition_start_time),
                modalities=get_modalities(nwbfile),  # TODO - include ISI data?
                code=None,
                notes=None,
                # TODO - add conditional for behavioral data to select appropriate devices
                active_devices=active_devices,
                configurations=[
                    *ephys_assembly_configs,
                    # The optotagging light-source config lives on the "Optotagging"
                    # stimulus epoch (see get_stimulation_epochs), not on the data stream.
                    # no lick spout / reward was included in these experiments
                ],
             ),
        ],
        stimulus_epochs=get_stimulation_epochs(nwbfile, session_info, session_start_time=acquisition_start_time),
        subject_details=AcquisitionSubjectDetails(
            mouse_platform_name="MindScope Running Disc",  # matches the Disc device in the instrument; de Vries et al. describe a rotating disk
        ),
    )

    return acquisition
