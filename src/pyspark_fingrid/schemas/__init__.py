"""
Fingrid dataset schemas package.

This package contains all dataset-specific schema implementations
and the schema registry for managing them.
"""

from .base import FingridDatasetSchema
from .registry import FingridSchemaRegistry
from .production import ElectricityProductionSchema
from .shortage import ElectricityShortageStatusSchema
from .consumption import ElectricityConsumptionSchema

# Auto-register all implemented schemas
FingridSchemaRegistry.register_schema(192, ElectricityProductionSchema)
FingridSchemaRegistry.register_schema(336, ElectricityShortageStatusSchema)
FingridSchemaRegistry.register_schema(363, ElectricityConsumptionSchema)

__all__ = [
    'FingridDatasetSchema',
    'FingridSchemaRegistry',
    'ElectricityProductionSchema',
    'ElectricityShortageStatusSchema',
    'ElectricityConsumptionSchema',
]