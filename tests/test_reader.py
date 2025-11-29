"""
Tests for the main data reader functionality.

This module contains tests for the read_fingrid_data function
and related API interaction functionality.
"""

import pytest
from unittest.mock import patch, Mock
from datetime import datetime

from pyspark_fingrid.reader import read_fingrid_data, list_available_datasets


class TestReadFingridData:
    """Test the main read_fingrid_data function."""

    def test_invalid_inputs(self):
        """Test validation of invalid inputs."""
        # Test missing API key
        with pytest.raises(ValueError, match="api_key is required"):
            read_fingrid_data("", 192)

        # Test invalid dataset_id type
        with pytest.raises(ValueError, match="dataset_id must be an integer"):
            read_fingrid_data("test-key", "192")

        # Test unsupported dataset
        with pytest.raises(ValueError, match="Dataset 999 not supported"):
            read_fingrid_data("test-key", 999)

    @patch('pyspark_fingrid.reader._fetch_metadata')
    @patch('pyspark_fingrid.reader._fetch_data')
    @patch('pyspark.sql.SparkSession.getActiveSession')
    def test_successful_data_read(self, mock_spark_session, mock_fetch_data, mock_fetch_metadata):
        """Test successful data reading and DataFrame creation."""
        # Mock Spark session
        mock_spark = Mock()
        mock_spark_session.return_value = mock_spark
        mock_df = Mock()
        mock_spark.createDataFrame.return_value = mock_df

        # Mock metadata response
        mock_fetch_metadata.return_value = {
            'nameEn': 'Test Dataset',
            'unitEn': 'MW',
            'updateCadenceEn': '3 min'
        }

        # Mock data response
        mock_fetch_data.return_value = [
            {
                'datasetId': 192,
                'startTime': '2024-07-24T12:00:00.000Z',
                'endTime': '2024-07-24T12:03:00.000Z',
                'value': 6789.5
            }
        ]

        # Test the function
        result = read_fingrid_data("test-key", 192)

        # Verify calls
        mock_fetch_metadata.assert_called_once_with("test-key", 192)
        mock_fetch_data.assert_called_once()
        mock_spark.createDataFrame.assert_called_once()

        # Verify result
        assert result == mock_df

    @patch('pyspark_fingrid.reader._fetch_data')
    def test_no_data_returned(self, mock_fetch_data):
        """Test handling when no data is returned from API."""
        mock_fetch_data.return_value = None

        result = read_fingrid_data("test-key", 192)
        assert result is None

    @patch('pyspark_fingrid.reader._fetch_data')
    @patch('pyspark.sql.SparkSession.getActiveSession')
    def test_no_spark_session(self, mock_spark_session, mock_fetch_data):
        """Test handling when no Spark session is available."""
        mock_spark_session.return_value = None
        mock_fetch_data.return_value = [{'test': 'data'}]

        result = read_fingrid_data("test-key", 192)
        assert result is None


class TestFetchMetadata:
    """Test metadata fetching functionality."""

    @patch('requests.get')
    def test_successful_metadata_fetch(self, mock_get):
        """Test successful metadata fetching."""
        from pyspark_fingrid.reader import _fetch_metadata

        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'nameEn': 'Test Dataset'}
        mock_get.return_value = mock_response

        result = _fetch_metadata("test-key", 192)

        assert result == {'nameEn': 'Test Dataset'}
        mock_get.assert_called_once_with(
            "https://data.fingrid.fi/api/datasets/192",
            headers={'x-api-key': 'test-key'},
            timeout=30
        )

    @patch('requests.get')
    def test_failed_metadata_fetch(self, mock_get):
        """Test failed metadata fetching."""
        from pyspark_fingrid.reader import _fetch_metadata

        # Mock failed response
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        result = _fetch_metadata("test-key", 192)
        assert result is None

    @patch('requests.get')
    def test_metadata_fetch_exception(self, mock_get):
        """Test metadata fetching with network exception."""
        from pyspark_fingrid.reader import _fetch_metadata

        mock_get.side_effect = Exception("Network error")

        result = _fetch_metadata("test-key", 192)
        assert result is None


class TestFetchData:
    """Test data fetching functionality."""

    @patch('requests.get')
    def test_successful_data_fetch(self, mock_get):
        """Test successful data fetching."""
        from pyspark_fingrid.reader import _fetch_data

        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': [{'test': 'record'}],
            'pagination': {'total': 1}
        }
        mock_get.return_value = mock_response

        result = _fetch_data("test-key", 192, "2024-07-24T00:00:00Z", "2024-07-24T01:00:00Z")

        assert result == [{'test': 'record'}]
        mock_get.assert_called_once()

    @patch('requests.get')
    @patch('time.sleep')
    def test_rate_limit_retry(self, mock_sleep, mock_get):
        """Test rate limit handling with retry."""
        from pyspark_fingrid.reader import _fetch_data

        # Mock rate limited response, then successful
        rate_limited_response = Mock()
        rate_limited_response.status_code = 429

        success_response = Mock()
        success_response.status_code = 200
        success_response.json.return_value = {'data': [{'test': 'record'}]}

        mock_get.side_effect = [rate_limited_response, success_response]

        result = _fetch_data("test-key", 192, "2024-07-24T00:00:00Z", "2024-07-24T01:00:00Z")

        assert result == [{'test': 'record'}]
        assert mock_get.call_count == 2
        mock_sleep.assert_called_once_with(10)

    @patch('requests.get')
    def test_api_error(self, mock_get):
        """Test API error handling."""
        from pyspark_fingrid.reader import _fetch_data

        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_get.return_value = mock_response

        result = _fetch_data("test-key", 192, "2024-07-24T00:00:00Z", "2024-07-24T01:00:00Z")
        assert result is None

    @patch('requests.get')
    def test_invalid_response_structure(self, mock_get):
        """Test handling of invalid response structure."""
        from pyspark_fingrid.reader import _fetch_data

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'invalid': 'structure'}
        mock_get.return_value = mock_response

        result = _fetch_data("test-key", 192, "2024-07-24T00:00:00Z", "2024-07-24T01:00:00Z")
        assert result is None


class TestListAvailableDatasets:
    """Test list_available_datasets function."""

    @patch('pyspark_fingrid.schemas.FingridSchemaRegistry.list_available_datasets')
    def test_list_available_datasets(self, mock_list):
        """Test that function delegates to registry."""
        list_available_datasets()
        mock_list.assert_called_once()