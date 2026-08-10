"""
Instrument metadata script for the Mesoscope 2-photon rig used to collect
the multiplane-ophys asset:
    multiplane-ophys_633532_2022-08-29_09-08-15

Instrument ID: 422_MESO2_20220218
"""

from aind_data_schema.components.coordinates import Axis, CoordinateSystem
from aind_data_schema.components.connections import Connection
from aind_data_schema.components.devices import (
    Camera,
    CameraAssembly,
    DAQDevice,
    Detector,
    Device,
    Disc,
    Filter,
    Laser,
    Lens,
    LickSpout,
    Microscope,
    Monitor,
    Objective
)
from aind_data_schema.components.identifiers import Software
from aind_data_schema.components.coordinates import Affine, Translation
from aind_data_schema_models.units import SizeUnit
from aind_data_schema.core.instrument import Instrument
from aind_data_schema_models.organizations import Organization
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.devices import DataInterface, DetectorType, ImmersionMedium, LickSensorType

# --- Coordinate system ---
coordinate_system = CoordinateSystem(
    name="BREGMA_ALS",
    origin="Bregma",
    axes=[
        Axis(name="X", direction="Posterior_to_anterior"),
        Axis(name="Y", direction="Right_to_left"),
        Axis(name="Z", direction="Down_to_up"),
    ],
    axis_unit="micrometer",
)

# --- Running disc ---
running_disc = Disc(
    name="MindScope Running Disk",
    manufacturer=Organization.AI,
    radius="8.255",
    radius_unit="centimeter",
    output="Digital Output",
    encoder="CUI Devices AMT102-V 0000 Dip Switch 2048 ppr",
    decoder="LS7366R",
    encoder_firmware=Software(name="ls7366r_quadrature_counter", version="0.1.6"),
    surface_material="Kittrich Magic Cover Solid Grip Liner",
)

microscope = Microscope(
    name="Multiscope",
    manufacturer=Organization.CUSTOM,
)

laser = Laser(
    name="Ti-Saph",
    wavelength=910,
    wavelength_unit=SizeUnit.NM,
    manufacturer=Organization.COHERENT_SCIENTIFIC,
    model="Chameleon Vision",
)

objective = Objective(
    name="Meso objective",
    manufacturer=Organization.JENOPTIK,
    model="14163000",
    serial_number="127",
    numerical_aperture=0.6,
    magnification=1,
    immersion=ImmersionMedium.OTHER,
    notes="Immersion medium used is ultrasound gel. Same refractive index as water."
)

detector = Detector(
    name="PMT",
    manufacturer=Organization.HAMAMATSU,
    model="H10770PA‑40",
    detector_type=DetectorType.PMT,
    data_interface=DataInterface.COAX,
)

# --- Stimulus monitor ---
monitor_coordinate_system = CoordinateSystem(
    name="SIPE_MONITOR_RTF",
    origin="Front_center",
    axes=[
        Axis(name="X", direction="Left_to_right"),
        Axis(name="Y", direction="Down_to_up"),
        Axis(name="Z", direction="Back_to_front"),
    ],
    axis_unit="millimeter",
)

stimulus_monitor = Monitor(
    name="Stimulus Screen",
    manufacturer=Organization.ASUS,
    model="PA248Q",
    relative_position=[],
    coordinate_system=monitor_coordinate_system,
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
    size_unit="pixel",
    viewing_distance="15.5",
    viewing_distance_unit="centimeter",
)

lickspout = LickSpout(
    name="Reward spout",
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

# --- Camera assembly coordinate system (shared) ---
camera_coordinate_system = CoordinateSystem(
    name="SIPE_CAMERA_RBF",
    origin="Front_center",
    axes=[
        Axis(name="X", direction="Left_to_right"),
        Axis(name="Y", direction="Up_to_down"),
        Axis(name="Z", direction="Back_to_front"),
    ],
    axis_unit="millimeter",
)

# --- Behavior camera assembly ---
behavior_camera_assembly = CameraAssembly(
    name="Behavior camera assembly",
    target="Body",
    relative_position=[],
    coordinate_system=camera_coordinate_system,
    transform=[
        Affine(affine_transform=[[-1, 0, 0], [0, 0, -1], [0, -3, 0]]),
        Translation(translation=[-0.03617, 0.23887, -0.02535]),
    ],
    camera=Camera(
        name="Behavior camera",
        manufacturer=Organization.ALLIED,
        model="Mako G-32B",
        detector_type="Camera",
        data_interface="Ethernet",
        cooling="No cooling",
        frame_rate="60",
        frame_rate_unit="hertz",
        chroma="Monochrome",
        sensor_width=658,
        sensor_height=492,
        size_unit="inch",
        sensor_format="1/3",
        sensor_format_unit="inch",
        bit_depth=8,
        bin_mode="No binning",
        gain="4",
        recording_software=Software(name="MultiVideoRecorder", version="1.1.7"),
        driver="Vimba",
        driver_version="Vimba GigE Transport Layer 1.6.0",
    ),
    lens=Lens(
        name="Behavior Camera Lens",
        manufacturer=Organization.THORLABS,
        model="MVL6WA",
    ),
    filter=Filter(
        name="Behavior Camera Filter",
        manufacturer=Organization.SEMROCK,
        model="FF01-747/33-25",
        filter_type="Band pass",
        cut_off_wavelength=780,
        cut_on_wavelength=714,
        center_wavelength=747,
        wavelength_unit="nanometer",
    ),
)

# --- Eye camera assembly ---
eye_camera_assembly = CameraAssembly(
    name="Eye camera assembly",
    target="Eye",
    relative_position=[],
    coordinate_system=camera_coordinate_system,
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
    camera=Camera(
        name="Eye camera",
        manufacturer=Organization.ALLIED,
        model="Mako G-32B",
        detector_type="Camera",
        data_interface="Ethernet",
        cooling="No cooling",
        frame_rate="60",
        frame_rate_unit="hertz",
        chroma="Monochrome",
        sensor_width=658,
        sensor_height=492,
        size_unit="inch",
        sensor_format="1/3",
        sensor_format_unit="inch",
        bit_depth=8,
        bin_mode="No binning",
        gain="4",
        recording_software=Software(name="MultiVideoRecorder", version="1.1.7"),
        driver="Vimba",
        driver_version="Vimba GigE Transport Layer 1.6.0",
    ),
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
        filter_type="Band pass",
        cut_off_wavelength=860,
        cut_on_wavelength=840,
        center_wavelength=850,
        wavelength_unit="nanometer",
    ),
)

# --- Face camera assembly ---
face_camera_assembly = CameraAssembly(
    name="Face camera assembly",
    target="Face",
    relative_position=[],
    coordinate_system=camera_coordinate_system,
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
    camera=Camera(
        name="Face camera",
        manufacturer=Organization.ALLIED,
        model="Mako G-32B",
        detector_type="Camera",
        data_interface="Ethernet",
        cooling="No cooling",
        frame_rate="60",
        frame_rate_unit="hertz",
        chroma="Monochrome",
        sensor_width=658,
        sensor_height=492,
        size_unit="inch",
        sensor_format="1/3",
        sensor_format_unit="inch",
        bit_depth=8,
        bin_mode="No binning",
        gain="4",
        recording_software=Software(name="MultiVideoRecorder", version="1.1.7"),
        driver="Vimba",
        driver_version="Vimba GigE Transport Layer 1.6.0",
    ),
    lens=Lens(
        name="Face Camera Lens",
        manufacturer=Organization.EDMUND_OPTICS,
        model="86-604",
    ),
    filter=Filter(
        name="Face Camera Filter",
        manufacturer=Organization.SEMROCK,
        model="FF01-715/LP-25",
        filter_type="Long pass",
        cut_on_wavelength=715,
        wavelength_unit="nanometer",
    ),
)

# --- DAQ devices ---
vbeb_daq = DAQDevice(
    name="VBEB DAQ",
    manufacturer=Organization.NATIONAL_INSTRUMENTS,
    model="USB-6001",
    data_interface="USB",
)

sync_daq = DAQDevice(
    name="SYNC DAQ",
    manufacturer=Organization.NATIONAL_INSTRUMENTS,
    model="PCIe-6612",
    data_interface="PCIe",
)

stim_daq = DAQDevice(
    name="STIM DAQ",
    manufacturer=Organization.NATIONAL_INSTRUMENTS,
    model="PCIe-6321",
    data_interface="PCIe",
)

# --- Instrument ---
instrument = Instrument(
    instrument_id="MESO1", #TODO:adjust 
    modification_date="2024-04-02", # TODO:Adjust
    modalities=[Modality.POPHYS, Modality.BEHAVIOR_VIDEOS, Modality.BEHAVIOR],
    coordinate_system=coordinate_system,
    components=[
        running_disc,
        stimulus_monitor,
        behavior_camera_assembly,
        eye_camera_assembly,
        face_camera_assembly,
        vbeb_daq,
        sync_daq,
        stim_daq,
        laser,
        lickspout,
        microscope,
        objective,
        detector,
    ],
)

if __name__ == "__main__":
    instrument.write_standard_file(prefix="mesoscope")