-- Dashboard Queries for Market Data Visualization
-- These queries are optimized for dashboard tools like Databricks SQL, Tableau, or Power BI

-- ============================================================================
-- MARKET OVERVIEW DASHBOARD
-- ============================================================================

-- 1. Market Summary (Last Trading Day)
SELECT 
    COUNT(DISTINCT symbol) AS total_symbols,
    AVG(close) AS avg_closing_price,
    SUM(volume) AS total_volume,
    AVG(daily_return) AS avg_daily_return,
    AVG(price_range) AS avg_price_range,
    SUM(CASE WHEN daily_return > 0 THEN 1 ELSE 0 END) AS gainers,
    SUM(CASE WHEN daily_return < 0 THEN 1 ELSE 0 END) AS losers,
    SUM(CASE WHEN daily_return = 0 THEN 1 ELSE 0 END) AS unchanged
FROM main.gold.daily_ohlcv
WHERE trade_date = (
    SELECT MAX(trade_date) FROM main.gold.daily_ohlcv
);

-- 2. Top Gainers and Losers (Last 7 Days)
WITH daily_returns AS (
    SELECT 
        symbol,
        trade_date,
        close,
        daily_return,
        volume,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date DESC) AS rn
    FROM main.gold.daily_ohlcv
    WHERE trade_date >= CURRENT_DATE() - INTERVAL 7 DAYS
)
SELECT 
    symbol,
    trade_date,
    close,
    daily_return,
    volume,
    CASE 
        WHEN daily_return > 0 THEN 'Gainer'
        WHEN daily_return < 0 THEN 'Loser'
        ELSE 'Unchanged'
    END AS category
FROM daily_returns
WHERE rn = 1
ORDER BY daily_return DESC
LIMIT 20;

-- ============================================================================
-- TECHNICAL ANALYSIS DASHBOARD
-- ============================================================================

-- 3. Moving Average Trends (Last 30 Days)
SELECT 
    d.symbol,
    d.trade_date,
    d.close,
    a.sma_5,
    a.sma_20,
    a.sma_50,
    CASE 
        WHEN d.close > a.sma_5 AND d.close > a.sma_20 AND d.close > a.sma_50 THEN 'Above All MAs'
        WHEN d.close < a.sma_5 AND d.close < a.sma_20 AND d.close < a.sma_50 THEN 'Below All MAs'
        ELSE 'Mixed'
    END AS trend_position,
    a.volatility
FROM main.gold.daily_ohlcv d
INNER JOIN main.gold.analytics a
    ON d.symbol = a.symbol AND d.trade_date = a.trade_date
WHERE d.trade_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND d.symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA')
ORDER BY d.symbol, d.trade_date;

-- 4. Volatility Heatmap Data
SELECT 
    symbol,
    trade_date,
    volatility,
    daily_return,
    CASE 
        WHEN volatility < 0.01 THEN 'Low'
        WHEN volatility < 0.03 THEN 'Medium'
        ELSE 'High'
    END AS volatility_category
FROM main.gold.daily_ohlcv d
INNER JOIN main.gold.analytics a
    ON d.symbol = a.symbol AND d.trade_date = a.trade_date
WHERE d.trade_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND a.volatility IS NOT NULL
ORDER BY symbol, trade_date;

-- ============================================================================
-- VOLUME ANALYSIS DASHBOARD
-- ============================================================================

-- 5. Volume Trends (Last 30 Days)
SELECT 
    symbol,
    trade_date,
    volume,
    close,
    daily_return,
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY trade_date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS avg_volume_20d,
    volume / AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY trade_date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS volume_ratio
FROM main.gold.daily_ohlcv
WHERE trade_date >= CURRENT_DATE() - INTERVAL 30 DAYS
ORDER BY symbol, trade_date;

-- 6. Volume Spike Detection
WITH volume_stats AS (
    SELECT 
        symbol,
        AVG(volume) AS avg_volume_30d,
        STDDEV(volume) AS stddev_volume_30d
    FROM main.gold.daily_ohlcv
    WHERE trade_date >= CURRENT_DATE() - INTERVAL 30 DAYS
    GROUP BY symbol
)
SELECT 
    d.symbol,
    d.trade_date,
    d.volume,
    d.close,
    d.daily_return,
    vs.avg_volume_30d,
    d.volume / vs.avg_volume_30d AS volume_multiple,
    CASE 
        WHEN d.volume > vs.avg_volume_30d * 2 THEN 'High Volume Spike'
        WHEN d.volume > vs.avg_volume_30d * 1.5 THEN 'Moderate Volume Spike'
        ELSE 'Normal Volume'
    END AS volume_category
FROM main.gold.daily_ohlcv d
INNER JOIN volume_stats vs ON d.symbol = vs.symbol
WHERE d.trade_date >= CURRENT_DATE() - INTERVAL 7 DAYS
  AND d.volume > vs.avg_volume_30d * 1.5
ORDER BY volume_multiple DESC;

-- ============================================================================
-- PERFORMANCE COMPARISON DASHBOARD
-- ============================================================================

-- 7. Stock Performance Comparison (Last 30 Days)
WITH performance_metrics AS (
    SELECT 
        symbol,
        FIRST_VALUE(close) OVER (
            PARTITION BY symbol 
            ORDER BY trade_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS start_price,
        LAST_VALUE(close) OVER (
            PARTITION BY symbol 
            ORDER BY trade_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS end_price,
        AVG(daily_return) OVER (PARTITION BY symbol) AS avg_daily_return,
        STDDEV(daily_return) OVER (PARTITION BY symbol) AS return_volatility,
        SUM(volume) OVER (PARTITION BY symbol) AS total_volume
    FROM main.gold.daily_ohlcv
    WHERE trade_date >= CURRENT_DATE() - INTERVAL 30 DAYS
)
SELECT DISTINCT
    symbol,
    start_price,
    end_price,
    (end_price - start_price) / start_price AS total_return_pct,
    avg_daily_return,
    return_volatility,
    total_volume
FROM performance_metrics
ORDER BY total_return_pct DESC;

-- 8. Sector Performance (Example: Tech Stocks)
SELECT 
    symbol,
    trade_date,
    close,
    daily_return,
    volume,
    AVG(daily_return) OVER (
        PARTITION BY DATE(trade_date)
        ORDER BY trade_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS sector_avg_return
FROM main.gold.daily_ohlcv
WHERE trade_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'INTC', 'AMD')
ORDER BY trade_date DESC, symbol;

-- ============================================================================
-- CORRELATION DASHBOARD
-- ============================================================================

-- 9. Stock Correlation Matrix Data
WITH returns_data AS (
    SELECT 
        symbol,
        trade_date,
        daily_return
    FROM main.gold.daily_ohlcv
    WHERE trade_date >= CURRENT_DATE() - INTERVAL 60 DAYS
      AND symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA')
)
SELECT 
    s1.symbol AS symbol1,
    s2.symbol AS symbol2,
    CORR(s1.daily_return, s2.daily_return) AS correlation,
    COUNT(*) AS observation_count
FROM returns_data s1
INNER JOIN returns_data s2
    ON s1.trade_date = s2.trade_date
    AND s1.symbol < s2.symbol
GROUP BY s1.symbol, s2.symbol
ORDER BY correlation DESC;

-- ============================================================================
-- TIME SERIES DASHBOARD
-- ============================================================================

-- 10. Price and Volume Time Series (Last 90 Days)
SELECT 
    symbol,
    trade_date,
    open,
    high,
    low,
    close,
    volume,
    daily_return,
    price_range,
    avg_price
FROM main.gold.daily_ohlcv
WHERE trade_date >= CURRENT_DATE() - INTERVAL 90 DAYS
  AND symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA')
ORDER BY symbol, trade_date;

-- 11. Moving Average Crossover Signals
WITH ma_data AS (
    SELECT 
        d.symbol,
        d.trade_date,
        d.close,
        a.sma_5,
        a.sma_20,
        a.sma_50,
        LAG(a.sma_5) OVER (PARTITION BY d.symbol ORDER BY d.trade_date) AS prev_sma_5,
        LAG(a.sma_20) OVER (PARTITION BY d.symbol ORDER BY d.trade_date) AS prev_sma_20,
        LAG(a.sma_50) OVER (PARTITION BY d.symbol ORDER BY d.trade_date) AS prev_sma_50
    FROM main.gold.daily_ohlcv d
    INNER JOIN main.gold.analytics a
        ON d.symbol = a.symbol AND d.trade_date = a.trade_date
    WHERE d.trade_date >= CURRENT_DATE() - INTERVAL 60 DAYS
)
SELECT 
    symbol,
    trade_date,
    close,
    sma_5,
    sma_20,
    sma_50,
    CASE 
        WHEN prev_sma_5 < prev_sma_20 AND sma_5 > sma_20 THEN 'Bullish Crossover (5/20)'
        WHEN prev_sma_5 > prev_sma_20 AND sma_5 < sma_20 THEN 'Bearish Crossover (5/20)'
        WHEN prev_sma_50 < prev_sma_20 AND sma_50 > sma_20 THEN 'Golden Cross (50/20)'
        WHEN prev_sma_50 > prev_sma_20 AND sma_50 < sma_20 THEN 'Death Cross (50/20)'
        ELSE 'No Signal'
    END AS signal_type
FROM ma_data
WHERE (
    (prev_sma_5 < prev_sma_20 AND sma_5 > sma_20) OR
    (prev_sma_5 > prev_sma_20 AND sma_5 < sma_20) OR
    (prev_sma_50 < prev_sma_20 AND sma_50 > sma_20) OR
    (prev_sma_50 > prev_sma_20 AND sma_50 < sma_20)
)
ORDER BY trade_date DESC, symbol;

-- ============================================================================
-- RISK ANALYSIS DASHBOARD
-- ============================================================================

-- 12. Risk Metrics by Symbol
SELECT 
    symbol,
    AVG(daily_return) AS avg_return,
    STDDEV(daily_return) AS volatility,
    MIN(daily_return) AS worst_day,
    MAX(daily_return) AS best_day,
    COUNT(*) AS trading_days,
    AVG(price_range / close) AS avg_intraday_volatility_pct
FROM main.gold.daily_ohlcv
WHERE trade_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY symbol
HAVING COUNT(*) >= 20
ORDER BY volatility DESC;

-- 13. Drawdown Analysis
WITH price_series AS (
    SELECT 
        symbol,
        trade_date,
        close,
        MAX(close) OVER (
            PARTITION BY symbol 
            ORDER BY trade_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS peak_price
    FROM main.gold.daily_ohlcv
    WHERE trade_date >= CURRENT_DATE() - INTERVAL 90 DAYS
)
SELECT 
    symbol,
    trade_date,
    close,
    peak_price,
    (close - peak_price) / peak_price AS drawdown_pct
FROM price_series
ORDER BY symbol, trade_date;

