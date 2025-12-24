"""
Yahoo Finance data source client.

Uses Yahoo Finance REST API to fetch historical stock market data.
No API keys required - uses public endpoints.
"""

import time
from datetime import datetime
from typing import List, Dict, Any, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .base_client import BaseMarketDataClient


class YahooFinanceClient(BaseMarketDataClient):
    """Client for fetching data from Yahoo Finance REST API.
    
    Uses the public Yahoo Finance v8 chart API endpoint which requires
    no authentication. Includes retry logic and rate limiting handling.
    """

    BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart"
    DEFAULT_TIMEOUT = 30
    MAX_RETRIES = 3
    RETRY_BACKOFF_FACTOR = 1.0

    def __init__(
        self,
        timeout: int = DEFAULT_TIMEOUT,
        max_retries: int = MAX_RETRIES,
        retry_backoff: float = RETRY_BACKOFF_FACTOR,
    ) -> None:
        """Initialize Yahoo Finance client.
        
        Args:
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            retry_backoff: Backoff factor for retries (seconds)
        
        Examples:
            >>> client = YahooFinanceClient()
            >>> client = YahooFinanceClient(timeout=60, max_retries=5)
        """
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create requests session with retry strategy.
        
        Returns:
            Configured requests Session with retry adapter
        """
        session = requests.Session()
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=self.retry_backoff,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _convert_interval(self, interval: str) -> str:
        """Convert standard interval to Yahoo Finance format.
        
        Args:
            interval: Standard interval (e.g., "1d", "1h", "5m")
        
        Returns:
            Yahoo Finance compatible interval string
        
        Examples:
            >>> client = YahooFinanceClient()
            >>> client._convert_interval("1d")
            '1d'
            >>> client._convert_interval("1h")
            '1h'
        """
        valid_intervals = ["1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo"]
        if interval not in valid_intervals:
            return "1d"
        return interval

    def fetch_bars(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        interval: str = "1d",
    ) -> List[Dict[str, Any]]:
        """Fetch bar data from Yahoo Finance.
        
        Args:
            symbol: Stock symbol (e.g., "AAPL", "MSFT")
            start_time: Start datetime for data range
            end_time: End datetime for data range
            interval: Bar interval (default: "1d")
        
        Returns:
            List of bar dictionaries with OHLCV data
        
        Raises:
            ValueError: If symbol is empty or time range is invalid
            ConnectionError: If API request fails after retries
        
        Examples:
            >>> client = YahooFinanceClient()
            >>> start = datetime(2024, 1, 1)
            >>> end = datetime(2024, 1, 31)
            >>> bars = client.fetch_bars("AAPL", start, end, "1d")
            >>> len(bars) > 0
            True
            >>> "symbol" in bars[0] and "timestamp" in bars[0]
            True
        """
        if not symbol or not symbol.strip():
            raise ValueError("Symbol cannot be empty")
        
        if start_time >= end_time:
            raise ValueError("start_time must be before end_time")
        
        yahoo_interval = self._convert_interval(interval)
        period1 = int(start_time.timestamp())
        period2 = int(end_time.timestamp())
        
        url = f"{self.BASE_URL}/{symbol}"
        params = {
            "period1": period1,
            "period2": period2,
            "interval": yahoo_interval,
        }
        
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            return self._parse_response(symbol, data)
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Failed to fetch data from Yahoo Finance: {e}") from e

    def _parse_response(self, symbol: str, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse Yahoo Finance API response into standardized format.
        
        Args:
            symbol: Stock symbol
            data: Raw API response JSON
        
        Returns:
            List of standardized bar dictionaries
        
        Raises:
            ValueError: If response format is invalid
        """
        if "chart" not in data or "result" not in data["chart"]:
            raise ValueError("Invalid response format from Yahoo Finance API")
        
        results = data["chart"]["result"]
        if not results:
            return []
        
        result = results[0]
        if "timestamp" not in result or "indicators" not in result:
            return []
        
        timestamps = result["timestamp"]
        indicators = result["indicators"]
        
        if "quote" not in indicators or not indicators["quote"]:
            return []
        
        quote = indicators["quote"][0]
        opens = quote.get("open", [])
        highs = quote.get("high", [])
        lows = quote.get("low", [])
        closes = quote.get("close", [])
        volumes = quote.get("volume", [])
        
        bars = []
        for i, ts in enumerate(timestamps):
            if closes[i] is None:
                continue
            
            bars.append({
                "symbol": symbol,
                "timestamp": datetime.fromtimestamp(ts),
                "open": float(opens[i]) if opens[i] is not None else closes[i],
                "high": float(highs[i]) if highs[i] is not None else closes[i],
                "low": float(lows[i]) if lows[i] is not None else closes[i],
                "close": float(closes[i]),
                "volume": int(volumes[i]) if volumes[i] is not None else 0,
            })
        
        return bars

    def get_available_symbols(self) -> List[str]:
        """Get list of available symbols.
        
        Note: Yahoo Finance doesn't provide a public endpoint for this.
        This method returns common symbols as a fallback.
        
        Returns:
            List of common stock symbols
        
        Examples:
            >>> client = YahooFinanceClient()
            >>> symbols = client.get_available_symbols()
            >>> "AAPL" in symbols
            True
        """
        return [
            "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA",
            "JPM", "V", "JNJ", "WMT", "PG", "MA", "UNH", "HD",
            "DIS", "BAC", "ADBE", "NFLX", "CRM", "PYPL", "INTC",
        ]

