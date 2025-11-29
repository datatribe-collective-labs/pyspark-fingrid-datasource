"""
Base abstract schema class for all Fingrid datasets.

This module defines the abstract interface that all dataset-specific
schemas must implement.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any, Optional

from pyspark.sql.types import StructType
from pyspark.sql import Row


class FingridDatasetSchema(ABC):
    """
    Abstract base class for Fingrid dataset schemas.

    Each dataset inherits from this and implements its specific schema
    and data transformation logic.

    Attributes:
        dataset_id (int): The Fingrid dataset ID
        metadata (dict): Dataset metadata from API
    """

    def __init__(self, dataset_id: int):
        """
        Initialize the schema handler.

        Args:
            dataset_id: The Fingrid dataset ID
        """
        self.dataset_id = dataset_id
        self.metadata: Optional[Dict[str, Any]] = None

    @abstractmethod
    def get_schema(self) -> StructType:
        """
        Return the PySpark schema for this dataset.

        Returns:
            StructType: PySpark schema with proper column names and types
        """
        pass

    @abstractmethod
    def transform_record(self, raw_record: Dict[str, Any]) -> Row:
        """
        Transform a raw API record into a PySpark Row.

        Args:
            raw_record: Raw record from Fingrid API

        Returns:
            Row: PySpark Row with dataset-specific schema
        """
        pass

    @abstractmethod
    def get_description(self) -> str:
        """
        Return a human-readable description of this dataset.

        Returns:
            str: Dataset description
        """
        pass

    def parse_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """
        Parse timestamp string to datetime object.

        Args:
            timestamp_str: ISO timestamp string

        Returns:
            datetime or None: Parsed timestamp
        """
        if not timestamp_str:
            return None
        try:
            if timestamp_str.endswith('Z'):
                return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            else:
                return datetime.fromisoformat(timestamp_str)
        except (ValueError, TypeError):
            return None

    def parse_float(self, value: Any) -> Optional[float]:
        """
        Safely parse value to float.

        Args:
            value: Value to parse

        Returns:
            float or None: Parsed float value
        """
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def parse_int(self, value: Any) -> Optional[int]:
        """
        Safely parse value to integer.

        Args:
            value: Value to parse

        Returns:
            int or None: Parsed integer value
        """
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def set_metadata(self, metadata: Dict[str, Any]) -> None:
        """
        Set metadata fetched from API.

        Args:
            metadata: Dataset metadata from Fingrid API
        """
        self.metadata = metadata

    def get_unit(self) -> str:
        """
        Get unit from metadata.

        Returns:
            str: Unit of measurement
        """
        if self.metadata:
            return self.metadata.get('unitEn', 'Unknown')
        return 'Unknown'

    def get_name(self) -> str:
        """
        Get dataset name from metadata.

        Returns:
            str: Dataset name
        """
        if self.metadata:
            return self.metadata.get('nameEn', f'Dataset {self.dataset_id}')
        return f'Dataset {self.dataset_id}'

    def get_update_frequency(self) -> str:
        """
        Get update frequency from metadata.

        Returns:
            str: Update frequency
        """
        if self.metadata:
            return self.metadata.get('updateCadenceEn', 'Unknown')
        return 'Unknown'