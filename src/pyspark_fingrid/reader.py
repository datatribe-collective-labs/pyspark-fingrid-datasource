"""
Main data reader for Fingrid datasets.

This module provides the primary interface for reading Fingrid data
with dataset-specific schemas and transformations.
"""

import time
import requests
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import DataFrame
from pyspark import SparkContext

from .schemas import FingridSchemaRegistry


def read_fingrid_data(
        api_key: str,
        dataset_id: int,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None
) -> Optional[DataFrame]:
    """
    Read Fingrid data with dataset-specific typing and schema.

    This function fetches data from the Fingrid API and returns a PySpark DataFrame
    with dataset-specific column names and types based on the registered schema.

    Args:
        api_key: Your Fingrid API key (get from https://data.fingrid.fi/en/)
        dataset_id: Dataset ID (must be registered in schema registry)
        start_time: ISO format "2024-07-24T00:00:00Z" (optional, defaults to last 30 min)
        end_time: ISO format "2024-07-24T06:00:00Z" (optional, defaults to now)

    Returns:
        DataFrame or None: PySpark DataFrame with dataset-specific schema

    Raises:
        ValueError: If dataset_id is not registered or API key is invalid

    Example:
        >>> df = read_fingrid_data("your-api-key", 192)
        >>> df.printSchema()
        root
         |-- startTime: timestamp (nullable = true)
         |-- endTime: timestamp (nullable = true)
         |-- production_mw: double (nullable = true)
         |-- datasetId: integer (nullable = true)
    """
    # Validate inputs
    if not api_key:
        raise ValueError("api_key is required")

    if not isinstance(dataset_id, int):
        raise ValueError("dataset_id must be an integer")

    # Get the appropriate schema for this dataset
    try:
        schema_handler = FingridSchemaRegistry.get_schema(dataset_id)
    except ValueError as e:
        print(f"❌ {e}")
        print("\nAvailable datasets:")
        FingridSchemaRegistry.list_available_datasets()
        raise

    # Default to last 30 minutes if no time specified
    if not start_time:
        now = datetime.now()
        start = now - timedelta(minutes=30)
        start_time = start.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_time = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    print(f"📊 Reading dataset {dataset_id}: {schema_handler.get_description()}")
    print(f"⏰ Time range: {start_time} to {end_time}")

    # Fetch metadata first
    metadata = _fetch_metadata(api_key, dataset_id)
    if metadata:
        schema_handler.set_metadata(metadata)
        print(f"📋 Name: {schema_handler.get_name()}")
        print(f"📏 Unit: {schema_handler.get_unit()}")
        print(f"🔄 Update frequency: {schema_handler.get_update_frequency()}")

    # Fetch data
    data = _fetch_data(api_key, dataset_id, start_time, end_time)
    if not data:
        return None

    # Transform data using dataset-specific schema
    print(f"🔄 Transforming data using {schema_handler.__class__.__name__}")

    schema = schema_handler.get_schema()
    rows = []

    for raw_record in data:
        try:
            transformed_row = schema_handler.transform_record(raw_record)
            rows.append(transformed_row)
        except Exception as e:
            print(f"⚠️  Error transforming record: {e}")
            continue

    if not rows:
        print("❌ No valid records after transformation")
        return None

    # Create DataFrame with proper schema
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if not spark:
            raise RuntimeError("No active Spark session found")

        df = spark.createDataFrame(rows, schema)
        print("✅ DataFrame created with dataset-specific schema!")
        return df

    except Exception as e:
        print(f"❌ Failed to create DataFrame: {e}")
        return None


def _fetch_metadata(api_key: str, dataset_id: int) -> Optional[dict]:
    """Fetch dataset metadata from Fingrid API."""
    try:
        metadata_url = f"https://data.fingrid.fi/api/datasets/{dataset_id}"
        headers = {'x-api-key': api_key}

        response = requests.get(metadata_url, headers=headers, timeout=30)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"⚠️  Could not fetch metadata (Status: {response.status_code})")
            return None
    except Exception as e:
        print(f"⚠️  Error fetching metadata: {e}")
        return None


def _fetch_data(api_key: str, dataset_id: int, start_time: str, end_time: str) -> Optional[list]:
    """Fetch dataset data from Fingrid API."""
    try:
        data_url = f"https://data.fingrid.fi/api/datasets/{dataset_id}/data"
        headers = {'x-api-key': api_key}
        params = {
            'startTime': start_time,
            'endTime': end_time,
            'format': 'json'
        }

        print(f"🔄 Calling: {data_url}")

        response = requests.get(data_url, headers=headers, params=params, timeout=60)
        print(f"   Status: {response.status_code}")

        # Handle rate limiting
        if response.status_code == 429:
            print("   ⚠️  Rate limited. Waiting 10 seconds...")
            time.sleep(10)
            response = requests.get(data_url, headers=headers, params=params, timeout=60)
            print(f"   Retry status: {response.status_code}")

        if response.status_code != 200:
            raise Exception(f"API error {response.status_code}: {response.text}")

        response_data = response.json()

        if 'data' not in response_data:
            raise Exception(f"Unexpected response structure: {list(response_data.keys())}")

        data = response_data['data']
        pagination = response_data.get('pagination', {})

        print(f"   ✅ Retrieved {len(data)} records")
        if pagination.get('total'):
            print(f"   📄 Total available: {pagination['total']} records")

        if not data:
            print("   ⚠️  No data returned")
            return None

        return data

    except Exception as e:
        print(f"❌ Failed to fetch data: {e}")
        return None


def list_available_datasets() -> None:
    """
    List all available datasets with descriptions.

    This is a convenience function that delegates to the schema registry.
    """
    FingridSchemaRegistry.list_available_datasets()