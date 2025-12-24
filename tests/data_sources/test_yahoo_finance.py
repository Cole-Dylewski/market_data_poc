"""
Tests for Yahoo Finance client.
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any

from src.data_sources.yahoo_finance import YahooFinanceClient


class TestYahooFinanceClient:
    """Test suite for YahooFinanceClient."""

    def test_init_default(self) -> None:
        """Test client initialization with default parameters."""
        client = YahooFinanceClient()
        assert client.timeout == 30
        assert client.max_retries == 3
        assert client.retry_backoff == 1.0
        assert client.session is not None

    def test_init_custom(self) -> None:
        """Test client initialization with custom parameters."""
        client = YahooFinanceClient(timeout=60, max_retries=5, retry_backoff=2.0)
        assert client.timeout == 60
        assert client.max_retries == 5
        assert client.retry_backoff == 2.0

    def test_convert_interval_valid(self) -> None:
        """Test interval conversion with valid intervals."""
        client = YahooFinanceClient()
        assert client._convert_interval("1d") == "1d"
        assert client._convert_interval("1h") == "1h"
        assert client._convert_interval("5m") == "5m"

    def test_convert_interval_invalid(self) -> None:
        """Test interval conversion with invalid interval defaults to 1d."""
        client = YahooFinanceClient()
        assert client._convert_interval("invalid") == "1d"

    def test_fetch_bars_empty_symbol(self) -> None:
        """Test fetch_bars raises ValueError for empty symbol."""
        client = YahooFinanceClient()
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 31)
        
        with pytest.raises(ValueError, match="Symbol cannot be empty"):
            client.fetch_bars("", start, end)
        
        with pytest.raises(ValueError, match="Symbol cannot be empty"):
            client.fetch_bars("   ", start, end)

    def test_fetch_bars_invalid_time_range(self) -> None:
        """Test fetch_bars raises ValueError for invalid time range."""
        client = YahooFinanceClient()
        start = datetime(2024, 1, 31)
        end = datetime(2024, 1, 1)
        
        with pytest.raises(ValueError, match="start_time must be before end_time"):
            client.fetch_bars("AAPL", start, end)
        
        with pytest.raises(ValueError, match="start_time must be before end_time"):
            client.fetch_bars("AAPL", start, start)

    @patch("src.data_sources.yahoo_finance.requests.Session")
    def test_fetch_bars_success(self, mock_session_class: Mock) -> None:
        """Test successful fetch_bars call."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "chart": {
                "result": [{
                    "timestamp": [1704067200, 1704153600],
                    "indicators": {
                        "quote": [{
                            "open": [150.0, 151.0],
                            "high": [152.0, 153.0],
                            "low": [149.0, 150.0],
                            "close": [151.0, 152.0],
                            "volume": [1000000, 1100000],
                        }]
                    }
                }]
            }
        }
        mock_response.raise_for_status = Mock()
        
        mock_session = Mock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session
        
        client = YahooFinanceClient()
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 31)
        
        bars = client.fetch_bars("AAPL", start, end, "1d")
        
        assert len(bars) == 2
        assert bars[0]["symbol"] == "AAPL"
        assert bars[0]["timestamp"] == datetime.fromtimestamp(1704067200)
        assert bars[0]["open"] == 150.0
        assert bars[0]["high"] == 152.0
        assert bars[0]["low"] == 149.0
        assert bars[0]["close"] == 151.0
        assert bars[0]["volume"] == 1000000
        
        mock_session.get.assert_called_once()
        call_args = mock_session.get.call_args
        assert "AAPL" in call_args[0][0]
        assert "period1" in call_args[1]["params"]
        assert "period2" in call_args[1]["params"]
        assert call_args[1]["params"]["interval"] == "1d"

    @patch("src.data_sources.yahoo_finance.requests.Session")
    def test_fetch_bars_api_error(self, mock_session_class: Mock) -> None:
        """Test fetch_bars handles API errors."""
        import requests
        
        mock_session = Mock()
        mock_session.get.side_effect = requests.exceptions.RequestException("Connection failed")
        mock_session_class.return_value = mock_session
        
        client = YahooFinanceClient()
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 31)
        
        with pytest.raises(ConnectionError, match="Failed to fetch data"):
            client.fetch_bars("AAPL", start, end)

    @patch("src.data_sources.yahoo_finance.requests.Session")
    def test_fetch_bars_empty_response(self, mock_session_class: Mock) -> None:
        """Test fetch_bars handles empty response."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "chart": {
                "result": []
            }
        }
        mock_response.raise_for_status = Mock()
        
        mock_session = Mock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session
        
        client = YahooFinanceClient()
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 31)
        
        bars = client.fetch_bars("AAPL", start, end)
        assert bars == []

    def test_parse_response_missing_data(self) -> None:
        """Test _parse_response handles missing data fields."""
        client = YahooFinanceClient()
        
        data = {
            "chart": {
                "result": [{
                    "timestamp": [1704067200],
                    "indicators": {
                        "quote": [{
                            "open": [None],
                            "high": [None],
                            "low": [None],
                            "close": [150.0],
                            "volume": [1000000],
                        }]
                    }
                }]
            }
        }
        
        bars = client._parse_response("AAPL", data)
        assert len(bars) == 1
        assert bars[0]["open"] == 150.0
        assert bars[0]["high"] == 150.0
        assert bars[0]["low"] == 150.0

    def test_parse_response_invalid_format(self) -> None:
        """Test _parse_response raises ValueError for invalid format."""
        client = YahooFinanceClient()
        
        with pytest.raises(ValueError, match="Invalid response format"):
            client._parse_response("AAPL", {})
        
        with pytest.raises(ValueError, match="Invalid response format"):
            client._parse_response("AAPL", {"chart": {}})

    def test_get_available_symbols(self) -> None:
        """Test get_available_symbols returns list of symbols."""
        client = YahooFinanceClient()
        symbols = client.get_available_symbols()
        
        assert isinstance(symbols, list)
        assert len(symbols) > 0
        assert "AAPL" in symbols
        assert "MSFT" in symbols
        assert all(isinstance(s, str) for s in symbols)

