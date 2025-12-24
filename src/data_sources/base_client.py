"""
Base class for market data source clients.

Defines the interface for all data source implementations to ensure consistency.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Any, Optional


class BaseMarketDataClient(ABC):
    """Base class for market data clients.
    
    All data source clients must implement the abstract methods defined here.
    This ensures a consistent interface across different data providers.
    """

    @abstractmethod
    def fetch_bars(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        interval: str = "1d",
    ) -> List[Dict[str, Any]]:
        """Fetch bar data (OHLCV) for a symbol in a time range.
        
        Args:
            symbol: Stock symbol (e.g., "AAPL", "MSFT")
            start_time: Start datetime for data range
            end_time: End datetime for data range
            interval: Bar interval (e.g., "1d", "1h", "5m")
        
        Returns:
            List of dictionaries, each containing:
                - symbol: Stock symbol
                - timestamp: Bar timestamp
                - open: Opening price
                - high: High price
                - low: Low price
                - close: Closing price
                - volume: Trading volume
        
        Raises:
            ValueError: If symbol is invalid or time range is invalid
            ConnectionError: If API request fails after retries
        """
        pass

    @abstractmethod
    def get_available_symbols(self) -> List[str]:
        """Get list of available symbols from this data source.
        
        Returns:
            List of available stock symbols
        
        Raises:
            ConnectionError: If API request fails
        """
        pass

