"""Generates acquisition metadata from NWB files for visual coding ephys sessions"""

import pandas as pd

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
    LightEmittingDiodeConfig,
)
from aind_data_schema.components.coordinates import Translation, CoordinateSystemLibrary
from aind_data_schema_models.units import SizeUnit, MassUnit
from aind_data_schema_models.stimulus_modality import StimulusModality

from mindscope_to_nwb_zarr.pynwb_utils import (
    get_data_stream_start_time,
    get_data_stream_end_time,
    get_modalities
)
from mindscope_to_nwb_zarr.aind_data_schema.utils import (
    build_probe_config,
    get_optostimulation_parameters,
    convert_intervals_to_stimulus_epochs,
)
from mindscope_to_nwb_zarr.aind_data_schema.visual_coding_ephys.instrument import (
    INSTRUMENT_ID,
    ephys_assembly_name,
    manipulator_name,
    probe_name,
    probe_letter_from_device_name,
    uses_optotagging_laser,
    optotagging_device_name,
    OPTOTAGGING_LASER_WAVELENGTH,
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

        # Convert table key to formatted stimulus name
        stimulus_name = table_key.replace('_', ' ').title()

        intervals_table_filtered = intervals_table.to_dataframe()
        stim_epoch = convert_intervals_to_stimulus_epochs(
            stimulus_name=stimulus_name,
            table_key=table_key,
            intervals_table=intervals_table_filtered,
            nwbfile=nwbfile,
            session_info=session_info
        )
        stimulation_epochs.append(stim_epoch)

    if 'optotagging' in nwbfile.processing:
        optogenetic_stimulation = nwbfile.processing['optotagging']['optogenetic_stimulation']
        session_id = int(session_info['id'])
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
                        coordinate_system=CoordinateSystemLibrary.MPM_MANIP_RFB,  # should be standardized (confirm relative to bregma, positions) @Saskia
                        local_axis_positions=Translation(translation=[0, 0, 0]),  # TODO - fill in with correct positions @Saskia
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
        subject_id=nwbfile.subject.subject_id,
        acquisition_start_time=nwbfile.session_start_time,
        acquisition_end_time=get_data_stream_end_time(nwbfile),
        ethics_review_id=None,  # TODO - obtain if available - YES, @Saskia
        instrument_id=INSTRUMENT_ID,  # matches the instrument file ("NP")
        acquisition_type=nwbfile.stimulus_notes,  # TODO - assert correct field for this data and present in both functional connectivity and brain observatory datasets
        notes=None,
        coordinate_system=CoordinateSystemLibrary.BREGMA_ARID,  # TODO - determine correct coordinate system library, will also be defined with instrument (not required to be same as acquisition)
        # coordinate system info might not be available, will check @Saskia
        # calibrations=[],
        # maintenance=[],
        data_streams=[
            DataStream(
                stream_start_time=get_data_stream_start_time(nwbfile),
                stream_end_time=get_data_stream_end_time(nwbfile),
                modalities=get_modalities(nwbfile),  # TODO - include ISI data?
                code=None,
                notes=None,
                # TODO - add conditional for behavioral data to select appropriate devices
                active_devices=active_devices,
                configurations=[
                    *ephys_assembly_configs,
                    # 473 nm laser for sessions with id >= 789848216, else 465 nm LED.
                    get_optotagging_config(session_id),  # TODO - should this go here or in the stimulation epochs configuration field?
                    # no lick spout / reward was included in these experiments
                ],
             ),
        ],
        stimulus_epochs=get_stimulation_epochs(nwbfile, session_info),
        subject_details=AcquisitionSubjectDetails(
            animal_weight_prior=None,  # TODO - pull in extra info if available - likely not available @Saskia
            animal_weight_post=None,
            weight_unit=MassUnit.G,
            mouse_platform_name="Running Wheel",
        ),
    )

    return acquisition
