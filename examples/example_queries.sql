-- Example SQL Queries for Market Data Analytics
-- These queries demonstrate how to use the Gold layer tables for analytics

-- ============================================================================
-- 1. DAILY RETURNS ANALYSIS
-- ============================================================================

-- Top 10 best performing stocks today
SELECT 
    symbol,
    trade_date,
    close,
    daily_return,
    volume,
    price_range
FROM main.gold.daily_ohlcv
WHERE trade_date = CURRENT_DATE() - INTERVAL 1 DAY
ORDER BY daily_return DESC
LIMIT 10;

-- Worst performing stocks today
SELECT 
    symbol,
    trade_date,
    close,
    daily_return,
    volume,
    price_range
FROM main.gold.daily_ohlcv
WHERE trade_date = CURRENT_DATE() - INTERVAL 1 DAY
ORDER BY daily_return ASC
LIMIT 10;

-- Average daily return by symbol over last 30 days
SELECT 
    symbol,
    AVG(daily_return) AS avg_daily_return,
    STDDEV(daily_return) AS return_volatility,
    COUNT(*) AS trading_days
FROM main.gold.daily_ohlcv
WHERE trade_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY symbol
HAVING COUNT(*) >= 20  -- At least 20 trading days
ORDER BY avg_daily_return DESC;

-- ============================================================================
-- 2. MOVING AVERAGE CROSSOVERS (Trading Signals)
-- ============================================================================

-- Golden Cross: SMA 50 crosses above SMA 200 (bullish signal)
-- Note: This example uses SMA 20 as proxy for SMA 200
WITH daily_with_sma AS (
    SELECT 
        symbol,
        trade_date,
        close,
        sma_5,
        sma_20,
        sma_50,
        LAG(sma_50) OVER (PARTITION BY symbol ORDER BY trade_date) AS prev_sma_50,
        LAG(sma_20) OVER (PARTITION BY symbol ORDER BY trade_date) AS prev_sma_20
    FROM main.gold.daily_ohlcv d
    INNER JOIN main.gold.analytics a
        ON d.symbol = a.symbol AND d.trade_date = a.trade_date
    WHERE d.trade_date >= CURRENT_DATE() - INTERVAL 60 DAYS
)
SELECT 
    symbol,
    trade_date,
    close,
    sma_20,
    sma_50,
    'Golden Cross' AS signal_type
FROM daily_with_sma
WHERE prev_sma_50 < prev_sma_20  -- Previous: 50 below 20
  AND sma_50 > sma_20            -- Current: 50 above 20
ORDER BY trade_date DESC, symbol;

-- Death Cross: SMA 50 crosses below SMA 20 (bearish signal)
WITH daily_with_sma AS (
    SELECT 
        symbol,
        trade_date,
        close,
        sma_5,
        sma_20,
        sma_50,
        LAG(sma_50) OVER (PARTITION BY symbol ORDER BY trade_date) AS prev_sma_50,
        LAG(sma_20) OVER (PARTITION BY symbol ORDER BY trade_date) AS prev_sma_20
    FROM main.gold.daily_ohlcv d
    INNER JOIN main.gold.analytics a
        ON d.symbol = a.symbol AND d.trade_date = a.trade_date
    WHERE d.trade_date >= CURRENT_DATE() - INTERVAL 60 DAYS
)
SELECT 
    symbol,
    trade_date,
    close,
    sma_20,
    sma_50,
    'Death Cross' AS signal_type
FROM daily_with_sma
WHERE prev_sma_50 > prev_sma_20  -- Previous: 50 above 20
  AND sma_50 < sma_20            -- Current: 50 below 20
ORDER BY trade_date DESC, symbol;

-- Stocks where price is above all moving averages (strong uptrend)
SELECT 
    d.symbol,
    d.trade_date,
    d.close,
    a.sma_5,
    a.sma_20,
    a.sma_50,
    CASE 
        WHEN d.close > a.sma_5 AND d.close > a.sma_20 AND d.close > a.sma_50 
        THEN 'Strong Uptrend'
        ELSE 'Not in Uptrend'
    END AS trend_status
FROM main.gold.daily_ohlcv d
INNER JOIN main.gold.analytics a
    ON d.symbol = a.symbol AND d.trade_date = a.trade_date
WHERE d.trade_date = CURRENT_DATE() - INTERVAL 1 DAY
  AND d.close > a.sma_5 
  AND d.close > a.sma_20 
  AND d.close > a.sma_50
ORDER BY d.close DESC;

-- ============================================================================
-- 3. VOLATILITY ANALYSIS
-- ============================================================================

-- Most volatile stocks (top 20 by volatility)
SELECT 
    symbol,
    trade_date,
    close,
    volatility,
    daily_return,
    price_range
FROM main.gold.daily_ohlcv d
INNER JOIN main.gold.analytics a
    ON d.symbol = a.symbol AND d.trade_date = a.trade_date
WHERE d.trade_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND a.volatility IS NOT NULL
ORDER BY a.volatility DESC
LIMIT 20;

-- Average volatility by symbol (last 30 days)
SELECT 
    symbol,
    AVG(volatility) AS avg_volatility,
    MAX(volatility) AS max_volatility,
    MIN(volatility) AS min_volatility,
    COUNT(*) AS days_with_data
FROM main.gold.analytics
WHERE trade_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND volatility IS NOT NULL
GROUP BY symbol
HAVING COUNT(*) >= 20
ORDER BY avg_volatility DESC;

-- ============================================================================
-- 4. VOLUME ANALYSIS
-- ============================================================================

-- Highest volume stocks today
SELECT 
    symbol,
    trade_date,
    close,
    volume,
    daily_return,
    num_bars
FROM main.gold.daily_ohlcv
WHERE trade_date = CURRENT_DATE() - INTERVAL 1 DAY
ORDER BY volume DESC
LIMIT 20;

-- Volume spike detection (volume > 2x average)
WITH volume_stats AS (
    SELECT 
        symbol,
        AVG(volume) AS avg_volume,
        STDDEV(volume) AS stddev_volume
    FROM main.gold.daily_ohlcv
    WHERE trade_date >= CURRENT_DATE() - INTERVAL 30 DAYS
    GROUP BY symbol
)
SELECT 
    d.symbol,
    d.trade_date,
    d.close,
    d.volume,
    vs.avg_volume,
    d.volume / vs.avg_volume AS volume_ratio,
    d.daily_return
FROM main.gold.daily_ohlcv d
INNER JOIN volume_stats vs ON d.symbol = vs.symbol
WHERE d.trade_date = CURRENT_DATE() - INTERVAL 1 DAY
  AND d.volume > vs.avg_volume * 2
ORDER BY volume_ratio DESC;

-- ============================================================================
-- 5. PRICE RANGE ANALYSIS
-- ============================================================================

-- Stocks with largest intraday price ranges (most volatile intraday)
SELECT 
    symbol,
    trade_date,
    open,
    high,
    low,
    close,
    price_range,
    price_range / close AS range_pct,
    daily_return
FROM main.gold.daily_ohlcv
WHERE trade_date >= CURRENT_DATE() - INTERVAL 7 DAYS
ORDER BY price_range DESC
LIMIT 20;

-- Average price range by symbol
SELECT 
    symbol,
    AVG(price_range) AS avg_price_range,
    AVG(price_range / close) AS avg_range_pct,
    MAX(price_range) AS max_price_range,
    COUNT(*) AS trading_days
FROM main.gold.daily_ohlcv
WHERE trade_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY symbol
HAVING COUNT(*) >= 20
ORDER BY avg_range_pct DESC;

-- ============================================================================
-- 6. CORRELATION ANALYSIS
-- ============================================================================

-- Calculate correlation between two stocks' daily returns
WITH stock_returns AS (
    SELECT 
        symbol,
        trade_date,
        daily_return
    FROM main.gold.daily_ohlcv
    WHERE trade_date >= CURRENT_DATE() - INTERVAL 60 DAYS
      AND symbol IN ('AAPL', 'MSFT')
)
SELECT 
    s1.symbol AS symbol1,
    s2.symbol AS symbol2,
    CORR(s1.daily_return, s2.daily_return) AS correlation,
    COUNT(*) AS days
FROM stock_returns s1
INNER JOIN stock_returns s2
    ON s1.trade_date = s2.trade_date
    AND s1.symbol < s2.symbol
GROUP BY s1.symbol, s2.symbol;

-- ============================================================================
-- 7. TECHNICAL INDICATOR COMBINATIONS
-- ============================================================================

-- Stocks with bullish technical setup:
-- - Price above all SMAs
-- - SMA 5 > SMA 20 (short-term uptrend)
-- - Low volatility (stable)
SELECT 
    d.symbol,
    d.trade_date,
    d.close,
    a.sma_5,
    a.sma_20,
    a.sma_50,
    a.volatility,
    d.daily_return
FROM main.gold.daily_ohlcv d
INNER JOIN main.gold.analytics a
    ON d.symbol = a.symbol AND d.trade_date = a.trade_date
WHERE d.trade_date = CURRENT_DATE() - INTERVAL 1 DAY
  AND d.close > a.sma_5
  AND d.close > a.sma_20
  AND d.close > a.sma_50
  AND a.sma_5 > a.sma_20
  AND a.volatility < 0.05  -- Low volatility
ORDER BY d.daily_return DESC;

-- ============================================================================
-- 8. PERFORMANCE COMPARISON
-- ============================================================================

-- Compare performance of multiple stocks over time period
SELECT 
    symbol,
    MIN(trade_date) AS start_date,
    MAX(trade_date) AS end_date,
    FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY trade_date) AS start_price,
    LAST_VALUE(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end_price,
    SUM(daily_return) AS total_return,
    AVG(daily_return) AS avg_daily_return,
    STDDEV(daily_return) AS return_volatility,
    COUNT(*) AS trading_days
FROM main.gold.daily_ohlcv
WHERE trade_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA')
GROUP BY symbol, close, daily_return, trade_date
ORDER BY symbol, trade_date;

-- ============================================================================
-- 9. DATA QUALITY CHECKS
-- ============================================================================

-- Check for missing data (gaps in trading days)
WITH expected_dates AS (
    SELECT 
        symbol,
        EXPLODE(SEQUENCE(
            DATE('2024-01-01'),
            CURRENT_DATE(),
            INTERVAL 1 DAY
        )) AS expected_date
    FROM (SELECT DISTINCT symbol FROM main.gold.daily_ohlcv)
    WHERE DAYOFWEEK(expected_date) NOT IN (1, 7)  -- Exclude weekends
),
actual_dates AS (
    SELECT DISTINCT symbol, DATE(trade_date) AS actual_date
    FROM main.gold.daily_ohlcv
)
SELECT 
    ed.symbol,
    ed.expected_date,
    CASE WHEN ad.actual_date IS NULL THEN 'Missing' ELSE 'Present' END AS status
FROM expected_dates ed
LEFT JOIN actual_dates ad
    ON ed.symbol = ad.symbol AND ed.expected_date = ad.actual_date
WHERE ad.actual_date IS NULL
ORDER BY ed.symbol, ed.expected_date;

-- ============================================================================
-- 10. SUMMARY STATISTICS
-- ============================================================================

-- Overall market statistics for last trading day
SELECT 
    COUNT(DISTINCT symbol) AS num_symbols,
    AVG(close) AS avg_close_price,
    AVG(daily_return) AS avg_daily_return,
    AVG(volume) AS avg_volume,
    AVG(price_range) AS avg_price_range,
    SUM(volume) AS total_volume,
    COUNT(*) AS total_trades
FROM main.gold.daily_ohlcv
WHERE trade_date = (
    SELECT MAX(trade_date) FROM main.gold.daily_ohlcv
);

