"""Custom stimulus models for Allen Brain Observatory optotagging experiments"""

from decimal import Decimal
from typing import List, Optional, Tuple

from aind_data_schema.base import GenericModel
from aind_data_schema.components.stimulus import PulseShape
from aind_data_schema_models.units import FrequencyUnit, TimeUnit
from pydantic import Field


class OptotaggingStimulation(GenericModel):
    """Description of optotagging stimulation parameters for Allen Brain Observatory experiments.

    This model is designed for optotagging experiments where individual pulses are delivered
    at varying light levels to identify optogenetically-tagged neurons.
    """

    stimulus_name: str = Field(..., title="Stimulus name")
    pulse_shape: PulseShape = Field(..., title="Pulse shape")
    pulse_durations: List[Decimal] = Field(
        ...,
        title="Pulse duration",
        description="Duration of each individual pulse"
    )
    pulse_durations_unit: TimeUnit = Field(default=TimeUnit.S, title="Pulse duration unit")
    ramp_duration: Decimal = Field(
        ...,
        title="Ramp duration",
        description="Duration of the ramp up and ramp down for a pulse"
    )
    ramp_duration_unit: TimeUnit = Field(default=TimeUnit.S, title="Ramp duration unit")
    inter_pulse_interval: Decimal = Field(
        ...,
        title="Inter-pulse interval",
        description="Time between consecutive pulses"
    )
    inter_pulse_interval_unit: TimeUnit = Field(default=TimeUnit.S, title="Inter-pulse interval unit")
    inter_pulse_interval_delay_range: Tuple[Decimal, Decimal] = Field(
        ...,
        title="Inter-pulse interval delay range",
        description="Uniformly distributed delay between (min, max)"
    )
    inter_pulse_interval_delay_range_unit: TimeUnit = Field(default=TimeUnit.S, title="Inter-pulse interval delay range unit")
    light_levels: List[Decimal] = Field(
        ...,
        title="Light levels",
        description="Relative light intensity levels used (arbitrary units)"
    )
    condition_description: str = Field(
        ...,
        title="Condition description",
        description="Description of the pulse condition"
    )
    notes: Optional[str] = Field(default=None, title="Notes")
