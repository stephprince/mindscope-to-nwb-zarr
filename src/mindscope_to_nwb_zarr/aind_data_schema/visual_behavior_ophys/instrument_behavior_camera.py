"""AIND metadata Instrument file for behavior and eye monitoring camera hardware"""

from datetime import date
from decimal import Decimal

from aind_data_schema.core.instrument import Instrument
from aind_data_schema.components.devices import (
    Camera,
    CameraAssembly,
    Filter,
    Lens,
    LightEmittingDiode,
)
from aind_data_schema.components.coordinates import (
    CoordinateSystem,
    Origin,
    Axis,
    AxisName,
    Direction,
)
from aind_data_schema_models.devices import (
    CameraTarget,
    DataInterface,
    FilterType,
)
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.organizations import Organization
from aind_data_schema_models.units import SizeUnit, FrequencyUnit


# Behavior Monitoring Hardware
behavior_camera = Camera(
    name="BehaviorCamera",
    manufacturer=Organization.ALLIED,
    model="Mako G-032B",
    serial_number=None,  # TODO: add serial number if available
    data_interface=DataInterface.ETH,  # GigE interface
    frame_rate=Decimal("30"),
    frame_rate_unit=FrequencyUnit.HZ,
    gain=Decimal("10"),
    notes="Allied Vision Mako G-032B with GigE interface. 33 ms exposure time.",
)

behavior_camera_lens = Lens(
    name="BehaviorCameraLens",
    manufacturer=Organization.THORLABS,
    model="MVL8M23",
    serial_number=None,  # TODO: add serial number if available
    additional_settings={
        "focal_length_mm": 8,
        "aperture": "f/1.4",
        "sensor_format": "2/3 inch C-Mount",
    },
    notes="8 mm EFL, f/1.4, for 2/3\" C-Mount Format Cameras, with lock",
)

behavior_camera_filter = Filter(
    name="BehaviorCameraFilter",
    manufacturer=Organization.SEMROCK,
    model="BSP01-785R-25",
    serial_number=None,  # TODO: add serial number if available
    filter_type=FilterType.SHORTPASS,
    cut_off_wavelength=785,
    wavelength_unit=SizeUnit.NM,
    notes="785 nm short-pass filter to suppress light from the eyetracking LED",
)

behavior_camera_assembly = CameraAssembly(
    name="BehaviorCameraAssembly",
    camera=behavior_camera,
    lens=behavior_camera_lens,
    filter=behavior_camera_filter,
    target=CameraTarget.BODY,
    relative_position=[],  # TODO: add relative position if available
)

# Behavior Illumination LED with filter
behavior_illumination_led = LightEmittingDiode(
    name="BehaviorIlluminationLED",
    manufacturer=Organization.OTHER,  # TODO: Consider adding LED Engine Inc. to Organization enum
    model="LZ4-40R308-0000",
    serial_number=None,  # TODO: add serial number if available
    wavelength=740,
    wavelength_unit=SizeUnit.NM,
    notes="LED Engine Inc. 740 nm illumination LED for behavior monitoring",
)

behavior_illumination_filter = Filter(
    name="BehaviorIlluminationFilter",
    manufacturer=Organization.THORLABS,
    model="LB1092-B-ML",  # TODO: this is a lens model, confirm filter model
    serial_number=None,  # TODO: add serial number if available
    filter_type=FilterType.BANDPASS,
    center_wavelength=747,
    wavelength_unit=SizeUnit.NM,
    notes=(
        "747+/-33 nm bandpass filter in front of behavior illumination LED "
        "to prevent visible portion of LED spectrum from reaching mouse eye"
    ),
)


# Eye Monitoring Hardware
eye_camera = Camera(
    name="EyeCamera",
    manufacturer=Organization.ALLIED,
    model="Mako G-032B",
    serial_number=None,  # TODO: add serial number if available
    data_interface=DataInterface.ETH,  # GigE interface
    frame_rate=Decimal("30"),
    frame_rate_unit=FrequencyUnit.HZ,
    gain=Decimal("10"),  # gain range 10-20, using lower bound - TODO: confirm exact gain value
    notes="Allied Vision Mako G-032B with GigE interface. 33 ms exposure time. Camera hardware gain of 10-20.",
)

eye_camera_lens = Lens(
    name="EyeCameraLens",
    manufacturer=Organization.INFINITY_PHOTO_OPTICAL,
    model="InfiniStix",
    serial_number=None,  # TODO: add serial number if available
    additional_settings={
        "working_distance_mm": 130,
        "magnification": 0.73,
    },
    notes="130 mm working distance, 0.73x magnification",
)

eye_camera_filter = Filter(
    name="EyeCameraFilter",
    manufacturer=Organization.SEMROCK,
    model="FF01-850/10-25",
    serial_number=None,  # TODO: add serial number if available
    filter_type=FilterType.BANDPASS,
    center_wavelength=850,
    wavelength_unit=SizeUnit.NM,
    notes="850+/-10 nm single-band bandpass filter",
)

eye_camera_dichroic = Filter(
    name="EyeCameraDichroic",
    manufacturer=Organization.SEMROCK,
    model="FF750-SDi02-25x36",
    serial_number=None,  # TODO: add serial number if available
    filter_type=FilterType.DICHROIC,
    wavelength_unit=SizeUnit.NM,
    notes="Dichroic used to fold beam path",
)

eye_camera_assembly = CameraAssembly(
    name="EyeCameraAssembly",
    camera=eye_camera,
    lens=eye_camera_lens,
    filter=eye_camera_filter,
    target=CameraTarget.EYE,
    relative_position=[],  # TODO: add relative position if available
)

# Eye Illumination LED
eye_illumination_led = LightEmittingDiode(
    name="EyeIlluminationLED",
    manufacturer=Organization.OTHER,  # TODO: Consider adding LED Engine Inc. to Organization enum
    model="LZ1-10R602-0000",
    serial_number=None,  # TODO: add serial number if available
    wavelength=850,
    wavelength_unit=SizeUnit.NM,
    notes="LED Engine Inc. 850 nm illumination LED for eye tracking",
)

eye_illumination_lens = Lens(
    name="EyeIlluminationLens",
    manufacturer=Organization.THORLABS,
    model="LB1092-B-ML",
    serial_number=None,  # TODO: add serial number if available
    notes="Lens in front of eye illumination LED",
)


# Define coordinate system for the instrument
# TODO: confirm coordinate system details
instrument_coordinate_system = CoordinateSystem(
    name="BehaviorCameraCoordinateSystem",
    origin=Origin.ORIGIN,
    axes=[
        Axis(name=AxisName.X, direction=Direction.LR),
        Axis(name=AxisName.Y, direction=Direction.AP),
        Axis(name=AxisName.Z, direction=Direction.IS),
    ],
    axis_unit=SizeUnit.MM,
)


# Create the Instrument - this will include the microscope, running wheel, etc. - one per session
# You can have devices in your instrument that you don't use in your experiment
behavior_camera_instrument = Instrument(
    instrument_id="BehaviorMonitoringRig",
    modification_date=date.today(),  # TODO: update as needed
    modalities=[Modality.BEHAVIOR_VIDEOS],
    coordinate_system=instrument_coordinate_system,
    components=[
        # Behavior monitoring camera assembly
        behavior_camera_assembly,
        # Behavior illumination components
        behavior_illumination_led,
        behavior_illumination_filter,
        # Eye monitoring camera assembly
        eye_camera_assembly,
        # Eye camera dichroic (additional filter in beam path)
        eye_camera_dichroic,
        # Eye illumination components
        eye_illumination_led,
        eye_illumination_lens,
    ],
    notes=(
        "Behavior and eye monitoring camera hardware for MindScope experiments. "
        "Behavior camera uses 740 nm LED illumination with 785 nm short-pass filter. "
        "Eye camera uses 850 nm LED illumination with 850+/-10 nm bandpass filter."
    ),
)


if __name__ == "__main__":
    from pathlib import Path

    repo_root = Path(__file__).parent.parent.parent.parent
    output_path = repo_root / "data/schema"
    output_path.mkdir(parents=True, exist_ok=True)

    serialized = behavior_camera_instrument.model_dump_json()
    deserialized = Instrument.model_validate_json(serialized)
    deserialized.write_standard_file(prefix=output_path / "instrument_behavior_camera")
