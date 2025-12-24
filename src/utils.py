"""
Utility functions for the market data pipeline.

Includes symbol scraping utilities, logging helpers, date/time utilities,
error handling decorators, retry logic, and data validation helpers.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, time, date
import time as time_module
import requests
from bs4 import BeautifulSoup

try:
    from .data_sources import YahooFinanceClient
except ImportError:
    # Handle case where running as standalone script
    from src.data_sources import YahooFinanceClient


def get_sp500_symbols() -> List[str]:
    """Fetch all S&P 500 stock symbols from stockanalysis.com.
    
    Scrapes the S&P 500 stocks list page and extracts all stock symbols
    from the table. Handles symbols with dots (e.g., BRK.B) and returns
    a deduplicated, sorted list.
    
    Returns:
        List of S&P 500 stock symbols (e.g., ['AAPL', 'MSFT', 'BRK.B'])
    
    Raises:
        requests.RequestException: If the HTTP request fails
        ValueError: If the page structure is unexpected and symbols cannot be extracted
    
    Examples:
        >>> symbols = get_sp500_symbols()
        >>> len(symbols) >= 500
        True
        >>> 'AAPL' in symbols
        True
        >>> 'BRK.B' in symbols
        True
    """
    url = "https://stockanalysis.com/list/sp-500-stocks/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        raise requests.RequestException(f"Failed to fetch S&P 500 list from {url}: {e}") from e
    
    soup = BeautifulSoup(response.text, 'html.parser')
    symbols: List[str] = []
    
    # Find the table containing stock symbols
    # The table has columns: No., Symbol, Company Name, Market Cap, Stock Price, % Change, Revenue
    tables = soup.find_all('table')
    
    if not tables:
        raise ValueError("No table found on the S&P 500 stocks page")
    
    # Process the first table (should be the main stocks table)
    table = tables[0]
    rows = table.find_all('tr')
    
    for row in rows:
        cells = row.find_all(['td', 'th'])
        if len(cells) < 2:
            continue
        
        # The symbol is typically in the second column (index 1)
        # It's usually in a link with href like /stocks/AAPL/
        symbol_cell = cells[1]
        
        # Try to extract from link first
        link = symbol_cell.find('a')
        if link:
            href = link.get('href', '')
            # Extract symbol from href like /stocks/AAPL/ or /stocks/BRK.B/
            if '/stocks/' in href:
                symbol = href.split('/stocks/')[1].rstrip('/').upper()
                if symbol and _is_valid_symbol(symbol):
                    symbols.append(symbol)
                    continue
        
        # Fallback: extract from cell text
        symbol_text = symbol_cell.get_text().strip().upper()
        if _is_valid_symbol(symbol_text):
            symbols.append(symbol_text)
    
    if not symbols:
        raise ValueError("No symbols found in the table. Page structure may have changed.")
    
    # Deduplicate and sort
    unique_symbols = sorted(list(set(symbols)))
    
    return unique_symbols


def _is_valid_symbol(symbol: str) -> bool:
    """Check if a string is a valid stock symbol.
    
    Valid symbols are:
    - 1-5 characters long
    - Uppercase letters, numbers, and dots only
    - At least one letter
    - Cannot start or end with a dot
    
    Args:
        symbol: String to validate
    
    Returns:
        True if the symbol is valid, False otherwise
    
    Examples:
        >>> _is_valid_symbol('AAPL')
        True
        >>> _is_valid_symbol('BRK.B')
        True
        >>> _is_valid_symbol('123')
        False
        >>> _is_valid_symbol('')
        False
        >>> _is_valid_symbol('TOOLONG')
        False
        >>> _is_valid_symbol('aapl')
        False
        >>> _is_valid_symbol('.A')
        False
    """
    if not symbol:
        return False
    
    if len(symbol) > 5:
        return False
    
    # Cannot start or end with a dot
    if symbol.startswith('.') or symbol.endswith('.'):
        return False
    
    # Must contain at least one letter
    if not any(c.isalpha() for c in symbol):
        return False
    
    # Only uppercase letters, numbers, and dots allowed
    # Check that all characters are uppercase letters, digits, or dots
    for c in symbol:
        if c == '.':
            continue
        if not c.isalnum():
            return False
        if c.isalpha() and not c.isupper():
            return False
    
    return True


def get_last_trading_day(reference_date: Optional[date] = None) -> date:
    """Get the last complete trading day (weekday) before the reference date.
    
    US stock markets are closed on weekends. This function finds the most recent
    weekday (Monday-Friday) before the reference date. Always goes back at least
    one day from the reference date to ensure we get a complete trading day.
    If reference_date is None, uses today's date.
    
    Note: This does not account for market holidays. For production use, consider
    integrating with a holiday calendar library.
    
    Args:
        reference_date: Date to start from. If None, uses today. Always returns
            a date before this reference date.
    
    Returns:
        The last complete trading day (weekday) date before the reference date
    
    Examples:
        >>> from datetime import date
        >>> # If today is Monday, returns Friday (last complete trading day)
        >>> last_day = get_last_trading_day()
        >>> last_day.weekday() < 5  # Monday=0, Friday=4
        True
    """
    if reference_date is None:
        reference_date = datetime.now().date()
    
    # Start from the day before the reference date
    # This ensures we get a complete trading day, not today's partial day
    current_date = reference_date - timedelta(days=1)
    max_days_back = 7  # Safety limit
    
    # Go back day by day until we find a weekday (Monday=0, Friday=4)
    for _ in range(max_days_back):
        # weekday() returns 0=Monday, 6=Sunday
        # We want Monday (0) through Friday (4)
        if current_date.weekday() < 5:  # Monday through Friday
            return current_date
        current_date = current_date - timedelta(days=1)
    
    # Fallback (shouldn't reach here)
    return current_date


def fetch_previous_day_5min_bars(
    symbols: List[str],
    client: Optional[YahooFinanceClient] = None,
    date: Optional[datetime] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch last trading day's 5-minute bar data for a list of symbols.
    
    Retrieves intraday data in 5-minute intervals for the last full trading day
    (or specified date) for all provided symbols. Automatically finds the most
    recent weekday (skips weekends). Market hours are assumed to be 9:30 AM to
    4:00 PM ET.
    
    Args:
        symbols: List of stock symbols to fetch data for
        client: Optional YahooFinanceClient instance. If None, creates a new one.
        date: Optional specific date to fetch. If None, finds the last trading day.
    
    Returns:
        Dictionary mapping symbol to list of bar dictionaries. Each bar contains:
        - symbol: Stock symbol
        - timestamp: Bar timestamp
        - open: Opening price
        - high: High price
        - low: Low price
        - close: Closing price
        - volume: Trading volume
        Symbols that fail to fetch will have empty lists.
    
    Raises:
        ValueError: If symbols list is empty
    
    Examples:
        >>> symbols = ['AAPL', 'MSFT']
        >>> data = fetch_previous_day_5min_bars(symbols)
        >>> 'AAPL' in data
        True
        >>> len(data['AAPL']) > 0
        True
        >>> 'timestamp' in data['AAPL'][0]
        True
    """
    if not symbols:
        raise ValueError("Symbols list cannot be empty")
    
    if client is None:
        client = YahooFinanceClient()
    
    # Determine the date to fetch
    if date is None:
        # Find the last trading day (skips weekends)
        # Go back at least 2-3 trading days to ensure data is available
        # (Yahoo Finance may have delays and rate limiting issues with very recent dates)
        last_trading_day = get_last_trading_day()
        today = datetime.now().date()
        
        # If the last trading day is very recent (within 2 days), go back further
        # This helps avoid rate limiting and ensures data availability
        if last_trading_day >= today - timedelta(days=2):
            # Go back 3 more trading days to ensure data is available
            target_date = get_last_trading_day(last_trading_day - timedelta(days=3))
        else:
            target_date = last_trading_day
    else:
        target_date = date.date() if isinstance(date, datetime) else date
    
    # Market hours: 9:30 AM to 4:00 PM ET (simplified to local time)
    # In production, you'd want to handle timezone conversion properly
    market_open = datetime.combine(target_date, time(9, 30))
    market_close = datetime.combine(target_date, time(16, 0))
    
    results: Dict[str, List[Dict[str, Any]]] = {}
    
    print(f"Fetching full trading day data (9:30 AM - 4:00 PM) for {len(symbols)} symbols...")
    print(f"Target date: {target_date}")
    print(f"Each symbol requires 1 API request for the full day's 5-minute bars\n")
    
    for i, symbol in enumerate(symbols):
        try:
            # Single request per symbol to get the full day's 5-minute bars
            # This is more efficient than multiple requests per symbol
            print(f"[{i+1}/{len(symbols)}] Fetching {symbol}...", end=" ", flush=True)
            bars = client.fetch_bars(
                symbol=symbol,
                start_time=market_open,
                end_time=market_close,
                interval="5m",
            )
            results[symbol] = bars
            print(f"✓ Got {len(bars)} bars")
            
            # Add delay between symbols to avoid rate limiting
            # Only delay if not the last symbol
            if i < len(symbols) - 1:
                delay = 3.0  # 3 second delay between symbols
                time_module.sleep(delay)
                
        except (ValueError, ConnectionError) as e:
            # Log error but continue with other symbols
            error_msg = str(e)
            if "429" in error_msg or "rate limit" in error_msg.lower() or "too many" in error_msg.lower():
                # If rate limited, wait longer before continuing
                wait_time = 15.0  # Wait 15 seconds on rate limit
                print(f"✗ Rate limited")
                print(f"  Waiting {wait_time} seconds before retrying {symbol}...")
                time_module.sleep(wait_time)
                # Try once more after waiting
                try:
                    bars = client.fetch_bars(
                        symbol=symbol,
                        start_time=market_open,
                        end_time=market_close,
                        interval="5m",
                    )
                    results[symbol] = bars
                    print(f"✓ Retry successful - Got {len(bars)} bars")
                    # Still add delay before next symbol
                    if i < len(symbols) - 1:
                        time_module.sleep(3.0)
                    continue
                except Exception as retry_error:
                    print(f"✗ Retry failed: {retry_error}")
                    pass  # If it still fails, continue with empty list
            else:
                print(f"✗ Error: {error_msg[:100]}")
            results[symbol] = []
            # Still add delay even on error to avoid compounding rate limits
            if i < len(symbols) - 1:
                time_module.sleep(3.0)
            continue
    
    print(f"\nCompleted fetching data for {len(symbols)} symbols")
    return results

