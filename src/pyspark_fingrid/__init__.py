"""
PySpark Fingrid Data Source

A Python package for reading Finnish electricity market data from Fingrid's API
into PySpark DataFrames with dataset-specific schemas and proper typing.

Main functions:
    read_fingrid_data: Read data with dataset-specific schema
    list_available_datasets: Show all supported datasets

Example:
    >>> from pyspark_fingrid import read_fingrid_data
    >>> df = read_fingrid_data("your-api-key", 192)
    >>> df.show()
"""

from .reader import read_fingrid_data, list_available_datasets
from .schemas import (
    FingridSchemaRegistry,
    FingridDatasetSchema,
    ElectricityProductionSchema,
    ElectricityShortageStatusSchema,
    ElectricityConsumptionSchema,
)

__version__ = "0.1.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

__all__ = [
    'read_fingrid_data',
    'list_available_datasets',
    'FingridSchemaRegistry',
    'FingridDatasetSchema',
    'ElectricityProductionSchema',
    'ElectricityShortageStatusSchema',
    'ElectricityConsumptionSchema',
]