"""Generates AIND Instrument metadata for the Visual Behavior Ophys dataset.

Three rig families are used in this dataset, selected per session by the
``equipment_name`` recorded in the session tables:

* ``BEH.*``   -> behavior box            (ported from ``reference/behavior_instrument.py``)
* ``CAM2P.*`` -> single-plane 2P rig     (ported from ``reference/cam2p_3_final_behavior_instrument.py``)
* ``MESO.1``  -> mesoscope 2P rig        (ported from ``reference/mesoscope_instrument.py``)

Selection is by *physical rig* (``equipment_name``), not by whether ophys data
was collected: a behavior-only training/habituation session run on an imaging
rig gets that rig's instrument. ``instrument_id`` is the exact ``equipment_name``.

The reference definitions were written against an older aind-data-schema and are
migrated here to the current schema (``global_coordinate_system`` on Instrument,
``local_coordinate_system`` on Monitor/CameraAssembly), mirroring the working
``visual_coding_ophys/instrument.py`` module.
"""

from datetime import date

import pandas as pd

from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.organizations import Organization
from aind_data_schema_models.units import FrequencyUnit, SizeUnit, UnitlessUnit
from aind_data_schema_models.devices import (
    BinMode,
    CameraChroma,
    CameraTarget,
    DaqChannelType,
    DetectorType,
    ImmersionMedium,
    LickSensorType,
)
from aind_data_schema_models.coordinates import (
    AnatomicalRelative,
    AxisName,
    Direction,
    Origin,
)

from aind_data_schema.components.coordinates import (
    Affine,
    Axis,
    CoordinateSystem,
    CoordinateSystemLibrary,
    Rotation,
    Scale,
    Translation,
)
from aind_data_schema.components.devices import (
    Camera,
    CameraAssembly,
    Cooling,
    DAQDevice,
    DataInterface,
    Detector,
    Device,
    Disc,
    Enclosure,
    Filter,
    FilterType,
    Laser,
    Lens,
    LickSpout,
    Microscope,
    Monitor,
    Objective,
)
from aind_data_schema.components.identifiers import Software
from aind_data_schema.core.instrument import Instrument

from mindscope_to_nwb_zarr.aind_data_schema.utils import resolve_instrument_id


# ---------------------------------------------------------------------------
# Canonical component names.
#
# Single source of truth for the device names used in the builders below, and
# imported by the acquisition modules so their configs reference devices that
# actually exist in the generated instrument.
# ---------------------------------------------------------------------------
DETECTOR_NAME = "PMT"                         # 2P + mesoscope
LASER_NAME = "Ti-Saph"                        # 2P + mesoscope
REWARD_SPOUT_NAME = "Reward Spout"            # all three rigs
STIMULUS_MONITOR_NAME = "Stimulus Screen"     # all three rigs
RUNNING_DISC_NAME = "MindScope Running Disc"  # all three rigs

BODY_CAMERA_NAME = "Body Camera"    # body monitoring: behavior box, 2P, mesoscope
TOP_CAMERA_NAME = "Top Camera"      # behavior box
EYE_CAMERA_NAME = "Eye Camera"      # 2P + mesoscope
FACE_CAMERA_NAME = "Face Camera"    # mesoscope

# Microscope device name per rig (mirrors visual_coding_ophys RIG_MICROSCOPE_NAMES).
# The microscope device is named after the rig's microscope, not the equipment id;
# the acquisition's ImagingConfig.device_name is matched to this name.
MICROSCOPE_NAMES = {
    "CAM2P.3": "Scientifica 1",
    "CAM2P.4": "Scientifica 2",
    "CAM2P.5": "Scientifica 3",
    "MESO.1": "Multiscope",
}


def microscope_name_for_equipment(equipment_name: str) -> str:
    """Microscope component name for a rig (see ``MICROSCOPE_NAMES``)."""
    try:
        return MICROSCOPE_NAMES[equipment_name]
    except KeyError:
        raise ValueError(
            f"No microscope name for equipment_name '{equipment_name}'. "
            f"Known: {sorted(MICROSCOPE_NAMES)}"
        )


def ophys_device_names(is_single_plane: bool) -> dict:
    """Canonical instrument device names an ophys acquisition should reference.

    Parameters
    ----------
    is_single_plane : bool
        True for the single-plane 2P (CAM2P) rig, False for the mesoscope (MESO.1).

    Returns
    -------
    dict
        Keys: ``detector``, ``laser``, ``body_camera``, ``eye_camera``,
        ``running_disc``, ``reward_spout``, and (mesoscope only) ``face_camera``.
    """
    names = {
        "detector": DETECTOR_NAME,
        "laser": LASER_NAME,
        "body_camera": BODY_CAMERA_NAME,
        "eye_camera": EYE_CAMERA_NAME,
        "running_disc": RUNNING_DISC_NAME,
        "reward_spout": REWARD_SPOUT_NAME,
    }
    if not is_single_plane:  # the mesoscope additionally has a face camera
        names["face_camera"] = FACE_CAMERA_NAME
    return names


# Reconstructed posthoc; dates are placeholders carried over from the reference
# files and should be corrected when better records are available.
_POSTHOC_NOTE = "Created several years posthoc from incomplete records. Much information is missing."
_BEHAVIOR_MODIFICATION_DATE = date(2016, 10, 12)  # TODO: confirm
_CAM2P_MODIFICATION_DATE = date(2016, 10, 12)  # TODO: confirm
_MESO_MODIFICATION_DATE = date(2024, 4, 2)  # TODO: confirm (from reference file)


# ---------------------------------------------------------------------------
# Shared component builders
# ---------------------------------------------------------------------------

def _build_running_disc() -> Disc:
    """The MindScope running disc, shared by all three rigs (behavior box, 2P, mesoscope).

    Uses the full mesoscope specification -- manufacturer AI, plus the encoder / decoder /
    firmware / surface-material details -- as the single, most complete record of the disc
    (the behavior box and 2P rigs use the same physical running disc).
    """
    return Disc(
        name=RUNNING_DISC_NAME,
        manufacturer=Organization.AI,
        radius=8.255,
        radius_unit=SizeUnit.CM,
        output=DaqChannelType.DO,
        encoder="CUI Devices AMT102-V 0000 Dip Switch 2048 ppr",
        decoder="LS7366R",
        encoder_firmware=Software(name="ls7366r_quadrature_counter", version="0.1.6"),
        surface_material="Kittrich Magic Cover Solid Grip Liner",
    )


def _build_stimulus_monitor() -> Monitor:
    """The ASUS stimulus monitor for the behavior box and 2P rigs.

    The mesoscope uses the same monitor hardware but defines it separately because
    its screen placement (viewing distance and transform) differs.
    """
    return Monitor(
        name=STIMULUS_MONITOR_NAME,
        serial_number=None,
        manufacturer=Organization.ASUS,
        model="PA248Q",
        notes="viewing distance is from screen normal to bregma",
        refresh_rate=60,
        width=1920,
        height=1200,
        size_unit=SizeUnit.PX,
        viewing_distance=17.0,
        viewing_distance_unit=SizeUnit.CM,
        relative_position=[AnatomicalRelative.ANTERIOR, AnatomicalRelative.RIGHT],
        contrast=30,
        contrast_unit=UnitlessUnit.PERCENT,
        brightness=50,
        brightness_unit=UnitlessUnit.PERCENT,
        local_coordinate_system=CoordinateSystemLibrary.SIPE_MONITOR_RTF,
        transform=[Rotation(angles=[45, 90, 0]), Translation(translation=[86.2, 118.6, 31.6])],
    )


def _build_reward_spout() -> LickSpout:
    """The reward spout / lick sensor assembly shared by all three rigs."""
    return LickSpout(
        name=REWARD_SPOUT_NAME,
        manufacturer=Organization.HAMILTON,
        model="8649-01",
        spout_diameter=0.672,
        solenoid_valve=Device(
            name="Solenoid valve",
            manufacturer=Organization.NRESEARCH_INC,
            model="161K011",
        ),
        lick_sensor=Device(
            name="Lick sensor",
            manufacturer=Organization.TE_CONNECTIVITY,
            model="1007079-1",
        ),
        lick_sensor_type=LickSensorType.PIEZOELECTIC,
    )


# ---------------------------------------------------------------------------
# Behavior box instrument (BEH.*)
# ---------------------------------------------------------------------------

def build_behavior_instrument(equipment_name: str) -> Instrument:
    """Build the behavior-box Instrument (ported from reference/behavior_instrument.py).

    The ``instrument_id`` is the compact ``[letter][number]`` box id (e.g. ``"BEH.G-Box6"``
    -> ``"G6"``), matching the Acquisition's ``instrument_id`` (see ``resolve_instrument_id``).
    """
    return Instrument(
        location="Unknown",
        instrument_id=resolve_instrument_id(equipment_name),
        modification_date=_BEHAVIOR_MODIFICATION_DATE,
        global_coordinate_system=CoordinateSystemLibrary.BREGMA_ARI,
        modalities=[Modality.BEHAVIOR],
        notes=_POSTHOC_NOTE,
        components=[
            _build_running_disc(),
            _build_stimulus_monitor(),
            Enclosure(
                name="Behavior box enclosure",
                size=Scale(scale=[0.585, 0.435, 0.4]),
                size_unit=SizeUnit.M,
                internal_material="Sound attenuating foam",
                external_material="Aluminium",
                grounded=True,
                laser_interlock=False,
                air_filtration=False,
            ),
            CameraAssembly(
                name="Body Camera Assembly",
                target=CameraTarget.BODY,
                relative_position=[AnatomicalRelative.LEFT],
                camera=Camera(
                    name=BODY_CAMERA_NAME,
                    detector_type=DetectorType.CAMERA,
                    manufacturer=Organization.ALLIED,
                    model="Mako G-032B",
                    data_interface=DataInterface.ETH,
                    cooling=Cooling.NO_COOLING,
                    frame_rate=30.0,
                    frame_rate_unit=FrequencyUnit.HZ,
                    chroma=CameraChroma.BW,
                ),
                lens=Lens(
                    name="Body Camera Lens",
                    manufacturer=Organization.THORLABS,
                    model="MVL8M23",
                ),
            ),
            CameraAssembly(
                name="Top Camera Assembly",
                target=CameraTarget.BODY,
                relative_position=[AnatomicalRelative.SUPERIOR],
                camera=Camera(
                    name=TOP_CAMERA_NAME,
                    detector_type=DetectorType.CAMERA,
                    manufacturer=Organization.ALLIED,
                    model="Mako G-032B",
                    data_interface=DataInterface.ETH,
                    cooling=Cooling.NO_COOLING,
                    frame_rate=30.0,
                    frame_rate_unit=FrequencyUnit.HZ,
                    chroma=CameraChroma.BW,
                ),
                lens=Lens(
                    name="Top Camera Lens",
                    manufacturer=Organization.THORLABS,
                    model="MVL8M23",
                ),
            ),
            _build_reward_spout(),
        ],
    )


# ---------------------------------------------------------------------------
# Single-plane 2P instrument (CAM2P.*)
# ---------------------------------------------------------------------------

def build_2p_instrument(equipment_name: str) -> Instrument:
    """Build the single-plane 2P Instrument (ported from reference/cam2p_3_final_behavior_instrument.py).

    The microscope component name is the per-rig name from ``MICROSCOPE_NAMES``
    (e.g. ``"Scientifica 1"``), matched by the ``ImagingConfig.device_name`` written
    by ``acquisition_behavior_ophys.py``. CAM2P.3/4/5 share this template (all Scientifica Vivoscope).
    """
    return Instrument(
        location="Unknown",
        instrument_id=resolve_instrument_id(equipment_name),
        modification_date=_CAM2P_MODIFICATION_DATE,
        global_coordinate_system=CoordinateSystemLibrary.BREGMA_ARI,
        modalities=[Modality.POPHYS, Modality.BEHAVIOR_VIDEOS],
        notes=_POSTHOC_NOTE,
        components=[
            Microscope(
                name=microscope_name_for_equipment(equipment_name),
                manufacturer=Organization.SCIENTIFICA,
                model="Vivoscope",
            ),
            Laser(
                name=LASER_NAME,
                wavelength=910,
                wavelength_unit=SizeUnit.NM,
                manufacturer=Organization.COHERENT_SCIENTIFIC,
                model="Chameleon Vision",
            ),
            Objective(
                name="Nikon 16x",
                numerical_aperture=0.8,
                magnification=16,
                manufacturer=Organization.NIKON,
                immersion=ImmersionMedium.WATER,
                model="N16XLWD-PF",
            ),
            Detector(
                name=DETECTOR_NAME,
                manufacturer=Organization.UNKNOWN,
                detector_type=DetectorType.PMT,
                data_interface=DataInterface.OTHER,
                notes="Unknown data interface",
            ),
            _build_stimulus_monitor(),
            _build_running_disc(),
            CameraAssembly(
                name="Eye Camera Assembly",
                target=CameraTarget.EYE,
                relative_position=[AnatomicalRelative.RIGHT],
                camera=Camera(
                    name=EYE_CAMERA_NAME,
                    detector_type=DetectorType.CAMERA,
                    manufacturer=Organization.ALLIED,
                    model="Mako G-032B",
                    data_interface=DataInterface.ETH,
                    cooling=Cooling.NO_COOLING,
                    frame_rate=30.0,
                    frame_rate_unit=FrequencyUnit.HZ,
                    chroma=CameraChroma.BW,
                ),
                lens=Lens(
                    name="Eye Camera Lens",
                    manufacturer=Organization.INFINITY_PHOTO_OPTICAL,
                    model="InfiniStix",
                ),
                filter=Filter(
                    name="Eye Camera Filter",
                    filter_type=FilterType.BANDPASS,
                    manufacturer=Organization.SEMROCK,
                    model="FF01-850/10-25",
                    center_wavelength=850,
                    wavelength_unit=SizeUnit.NM,
                ),
            ),
            Filter(
                name="Eye Camera Dichroic",
                filter_type=FilterType.DICHROIC,
                manufacturer=Organization.SEMROCK,
                model="FF750-SDi02-25x36",
            ),
            CameraAssembly(
                name="Body Camera Assembly",
                target=CameraTarget.BODY,
                relative_position=[AnatomicalRelative.LEFT, AnatomicalRelative.POSTERIOR],
                camera=Camera(
                    name=BODY_CAMERA_NAME,
                    detector_type=DetectorType.CAMERA,
                    manufacturer=Organization.ALLIED,
                    model="Mako G-032B",
                    data_interface=DataInterface.ETH,
                    cooling=Cooling.NO_COOLING,
                    frame_rate=30.0,
                    frame_rate_unit=FrequencyUnit.HZ,
                    chroma=CameraChroma.BW,
                ),
                lens=Lens(
                    name="Body Camera Lens",
                    manufacturer=Organization.THORLABS,
                    model="MVL8M23",
                ),
                filter=Filter(
                    name="Body Camera Filter",
                    manufacturer=Organization.SEMROCK,
                    model="BSP01-785R-25",
                    filter_type=FilterType.SHORTPASS,
                    cut_off_wavelength=785,
                    wavelength_unit=SizeUnit.NM,
                ),
            ),
            _build_reward_spout(),
        ],
    )


# ---------------------------------------------------------------------------
# Mesoscope instrument (MESO.1)
# ---------------------------------------------------------------------------

# The mesoscope monitoring cameras are expressed in the standard SIPE camera frame
# (front-center origin, RBF axes); use the shared library definition.
_MESO_CAMERA_COORDINATE_SYSTEM = CoordinateSystemLibrary.SIPE_CAMERA_RBF


def _build_meso_camera(name: str) -> Camera:
    """Build a mesoscope monitoring camera (Allied Mako G-032B, 60 Hz).

    The mesoscope's body, eye, and face cameras share these settings and differ
    only by ``name`` (and their assembly position/transform).
    """
    return Camera(
        name=name,
        manufacturer=Organization.ALLIED,
        model="Mako G-032B",
        detector_type=DetectorType.CAMERA,
        data_interface=DataInterface.ETH,
        cooling=Cooling.NO_COOLING,
        frame_rate=60.0,
        frame_rate_unit=FrequencyUnit.HZ,
        chroma=CameraChroma.BW,
        sensor_width=658,
        sensor_height=492,
        size_unit=SizeUnit.IN,
        sensor_format="1/3",
        sensor_format_unit=SizeUnit.IN,
        bit_depth=8,
        bin_mode=BinMode.NO_BINNING,
        gain=4.0,
        recording_software=Software(name="MultiVideoRecorder", version="1.1.7"),
        driver="Vimba",
        driver_version="Vimba GigE Transport Layer 1.6.0",
    )


def build_mesoscope_instrument(equipment_name: str) -> Instrument:
    """Build the mesoscope Instrument (ported from reference/mesoscope_instrument.py).

    The microscope component name is the per-rig name from ``MICROSCOPE_NAMES``
    (``"Multiscope"``), matched by the ``ImagingConfig.device_name`` written by
    ``acquisition_behavior_ophys.py``.
    """
    coordinate_system = CoordinateSystem(
        name="BREGMA_ALS",
        origin=Origin.BREGMA,
        axes=[
            Axis(name=AxisName.X, direction=Direction.PA),
            Axis(name=AxisName.Y, direction=Direction.RL),
            Axis(name=AxisName.Z, direction=Direction.DU),
        ],
        axis_unit=SizeUnit.UM,
    )

    running_disc = _build_running_disc()

    stimulus_monitor = Monitor(
        name=STIMULUS_MONITOR_NAME,
        manufacturer=Organization.ASUS,
        model="PA248Q",
        relative_position=[],
        local_coordinate_system=CoordinateSystemLibrary.SIPE_MONITOR_RTF,
        transform=[
            Affine(
                affine_transform=[
                    [-0.80914, -0.58761, 0],
                    [-0.12391, 0.17063, 0.97751],
                    [-0.5744, 0.79095, -0.21087],
                ]
            ),
            Translation(translation=[0.08751, -0.12079, 0.02298]),
        ],
        notes="viewing distance is from screen normal to bregma",
        refresh_rate=60,
        width=1920,
        height=1200,
        size_unit=SizeUnit.PX,
        viewing_distance=15.5,
        viewing_distance_unit=SizeUnit.CM,
    )

    behavior_camera_assembly = CameraAssembly(
        name="Body Camera Assembly",
        target=CameraTarget.BODY,
        relative_position=[],
        local_coordinate_system=_MESO_CAMERA_COORDINATE_SYSTEM,
        transform=[
            Affine(affine_transform=[[-1, 0, 0], [0, 0, -1], [0, -3, 0]]),
            Translation(translation=[-0.03617, 0.23887, -0.02535]),
        ],
        camera=_build_meso_camera(BODY_CAMERA_NAME),
        lens=Lens(
            name="Body Camera Lens",
            manufacturer=Organization.THORLABS,
            model="MVL6WA",
        ),
        filter=Filter(
            name="Body Camera Filter",
            manufacturer=Organization.SEMROCK,
            model="FF01-747/33-25",
            filter_type=FilterType.BANDPASS,
            cut_off_wavelength=780,
            cut_on_wavelength=714,
            center_wavelength=747,
            wavelength_unit=SizeUnit.NM,
        ),
    )

    eye_camera_assembly = CameraAssembly(
        name="Eye Camera Assembly",
        target=CameraTarget.EYE,
        relative_position=[],
        local_coordinate_system=_MESO_CAMERA_COORDINATE_SYSTEM,
        transform=[
            Affine(
                affine_transform=[
                    [-0.5, -0.86603, 0],
                    [-0.366, 0.21131, -0.90631],
                    [0.78489, -0.45315, -0.42262],
                ]
            ),
            Translation(translation=[-0.14259, 0.06209, -0.09576]),
        ],
        camera=_build_meso_camera(EYE_CAMERA_NAME),
        lens=Lens(
            name="Eye Camera Lens",
            manufacturer=Organization.OTHER,  # Infinity Photo-Optical (not in enum)
            model="213073",
            notes="Manufacturer is Infinity Photo-Optical (not in Organization enum).",
        ),
        filter=Filter(
            name="Eye Camera Filter",
            manufacturer=Organization.SEMROCK,
            model="FF01-850/10-25",
            filter_type=FilterType.BANDPASS,
            cut_off_wavelength=860,
            cut_on_wavelength=840,
            center_wavelength=850,
            wavelength_unit=SizeUnit.NM,
        ),
    )

    face_camera_assembly = CameraAssembly(
        name="Face Camera Assembly",
        target=CameraTarget.FACE,
        relative_position=[],
        local_coordinate_system=_MESO_CAMERA_COORDINATE_SYSTEM,
        transform=[
            Affine(
                affine_transform=[
                    [-0.17365, 0.98481, 0],
                    [0.44709, 0.07883, -0.89101],
                    [-0.87747, -0.15472, -0.45399],
                ]
            ),
            Translation(translation=[0.154, 0.03078, 0.06346]),
        ],
        camera=_build_meso_camera(FACE_CAMERA_NAME),
        lens=Lens(
            name="Face Camera Lens",
            manufacturer=Organization.EDMUND_OPTICS,
            model="86-604",
        ),
        filter=Filter(
            name="Face Camera Filter",
            manufacturer=Organization.SEMROCK,
            model="FF01-715/LP-25",
            filter_type=FilterType.LONGPASS,
            cut_on_wavelength=715,
            wavelength_unit=SizeUnit.NM,
        ),
    )

    return Instrument(
        instrument_id=resolve_instrument_id(equipment_name),
        modification_date=_MESO_MODIFICATION_DATE,
        modalities=[Modality.POPHYS, Modality.BEHAVIOR_VIDEOS, Modality.BEHAVIOR],
        notes=_POSTHOC_NOTE,
        global_coordinate_system=coordinate_system,
        components=[
            running_disc,
            stimulus_monitor,
            behavior_camera_assembly,
            eye_camera_assembly,
            face_camera_assembly,
            DAQDevice(
                name="VBEB DAQ",
                manufacturer=Organization.NATIONAL_INSTRUMENTS,
                model="USB-6001",
                data_interface=DataInterface.USB,
            ),
            DAQDevice(
                name="SYNC DAQ",
                manufacturer=Organization.NATIONAL_INSTRUMENTS,
                model="PCIe-6612",
                data_interface=DataInterface.PCIE,
            ),
            DAQDevice(
                name="STIM DAQ",
                manufacturer=Organization.NATIONAL_INSTRUMENTS,
                model="PCIe-6321",
                data_interface=DataInterface.PCIE,
            ),
            Laser(
                name=LASER_NAME,
                wavelength=910,
                wavelength_unit=SizeUnit.NM,
                manufacturer=Organization.COHERENT_SCIENTIFIC,
                model="Chameleon Vision",
            ),
            _build_reward_spout(),
            Microscope(
                name=microscope_name_for_equipment(equipment_name),
                manufacturer=Organization.CUSTOM,
            ),
            Objective(
                name="Meso objective",
                manufacturer=Organization.JENOPTIK,
                model="14163000",
                serial_number="127",
                numerical_aperture=0.6,
                magnification=1,
                immersion=ImmersionMedium.OTHER,
                notes="Immersion medium used is ultrasound gel. Same refractive index as water.",
            ),
            Detector(
                name=DETECTOR_NAME,
                manufacturer=Organization.HAMAMATSU,
                model="H10770PA-40",
                detector_type=DetectorType.PMT,
                data_interface=DataInterface.COAX,
            ),
        ],
    )


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

def generate_instrument(session_info: pd.Series) -> Instrument:
    """Build the correct Instrument for a session based on its ``equipment_name``.

    Works for both behavior-only rows (from ``behavior_session_table``) and ophys
    rows (from ``ophys_experiment_table``); both carry ``equipment_name``.

    Raises
    ------
    ValueError
        If ``equipment_name`` does not match a known rig family.
    """
    equipment_name = str(session_info["equipment_name"])

    if equipment_name.startswith("BEH"):
        return build_behavior_instrument(equipment_name)
    if equipment_name.startswith("CAM2P"):
        return build_2p_instrument(equipment_name)
    if equipment_name.startswith("MESO"):
        return build_mesoscope_instrument(equipment_name)

    raise ValueError(
        f"No instrument definition for equipment_name '{equipment_name}'. "
        "Expected a name starting with 'BEH', 'CAM2P', or 'MESO'."
    )
