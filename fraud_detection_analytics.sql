-- ============================================================================
-- TABLE OF CONTENTS
-- ============================================================================
-- 1. DATABASE SCHEMA DESIGN (Star Schema)
--    1.1 dim_cards         - Card dimension table
--    1.2 dim_identity      - Device & identity fingerprint dimension
--    1.3 fact_transactions  - Core transaction fact table
--    1.4 Performance Indexes
--
-- 2. ADVANCED ANALYTICAL QUERIES
--    2.1 Multi-Window Transaction Velocity & Burst Detection
--    2.2 Recursive CTE: Fraud Chain & Cascading Fraud Detection
--    2.3 Stored Procedure: Dynamic Risk Scoring Engine
--    2.4 Materialized View: Real-Time Merchant Risk Dashboard
--    2.5 Dynamic Pivot: Time-Series Fraud Heatmap
--
-- 3. UTILITY QUERIES
--    3.1 Portfolio-Level Fraud KPI Summary
--    3.2 Rolling 7-Day Fraud Trend
--    3.3 Card-Level Risk Profile
-- ============================================================================


-- ============================================================================
-- SECTION 1: DATABASE SCHEMA DESIGN
-- Architecture: Star Schema optimized for OLAP analytical workloads
-- The fact table (transactions) sits at the center, with dimension tables
-- for cards and identity/device information branching out.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS fraud_analytics;
COPY fact_transactions FROM '/path/to/data.csv'
WITH (FORMAT csv, HEADER true);
SET search_path TO fraud_analytics;

-- ────────────────────────────────────────────────────────────────────────────
-- 1.1 DIMENSION TABLE: Card Information
-- Stores static card attributes. Each card has a unique hash, type,
-- issuing bank, and a dynamic risk_tier that can be updated as new
-- fraud intelligence becomes available.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE dim_cards (
    card_id         SERIAL PRIMARY KEY,
    card_hash       VARCHAR(64) NOT NULL,                       -- Tokenized card number (PCI compliant)
    card_type       VARCHAR(20) CHECK (card_type IN 
                        ('credit', 'debit', 'mastercard', 'visa', 'amex')),
    card_network    VARCHAR(30),                                 -- Visa, Mastercard, Amex, etc.
    issuing_bank    VARCHAR(100),                                -- Bank that issued the card
    bin_country     CHAR(2),                                     -- Country from BIN lookup (ISO 3166)
    enrollment_date DATE,                                        -- When the card was first enrolled
    risk_tier       SMALLINT DEFAULT 1 
                        CHECK (risk_tier BETWEEN 1 AND 5),       -- 1=Low, 5=Critical risk
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ────────────────────────────────────────────────────────────────────────────
-- 1.2 DIMENSION TABLE: Device & Identity Fingerprints
-- Captures the digital footprint of each transaction session.
-- Device fingerprinting is a key fraud signal — the same device used
-- across multiple stolen cards is a strong indicator of organized fraud.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE dim_identity (
    identity_id     SERIAL PRIMARY KEY,
    device_type     VARCHAR(30),                                 -- mobile, desktop, tablet
    device_info     VARCHAR(100),                                -- Device model / fingerprint hash
    ip_address      INET,                                        -- PostgreSQL native IP type
    email_domain    VARCHAR(100),                                -- Email provider domain
    browser         VARCHAR(50),                                 -- Chrome, Firefox, Safari, etc.
    os_type         VARCHAR(50),                                 -- Windows, macOS, Android, iOS
    screen_res      VARCHAR(20),                                 -- Screen resolution (e.g., 1920x1080)
    geo_latitude    DECIMAL(9,6),                                -- GPS latitude of transaction
    geo_longitude   DECIMAL(9,6),                                -- GPS longitude of transaction
    is_proxy        BOOLEAN DEFAULT FALSE                        -- Whether IP is a known proxy/VPN
);

-- ────────────────────────────────────────────────────────────────────────────
-- 1.3 FACT TABLE: Transactions
-- The core analytical table. Every row represents one transaction event.
-- This table is designed for high-volume analytical queries with 
-- appropriate data types and constraints for data integrity.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE fact_transactions (
    txn_id              BIGSERIAL PRIMARY KEY,
    card_id             INT REFERENCES dim_cards(card_id),       -- FK to card dimension
    identity_id         INT REFERENCES dim_identity(identity_id),-- FK to identity dimension
    txn_timestamp       TIMESTAMP NOT NULL,                      -- When the transaction occurred
    txn_amount          DECIMAL(12,2) NOT NULL 
                            CHECK (txn_amount > 0),              -- Transaction amount in USD
    product_category    VARCHAR(50),                             -- What was purchased
    merchant_id         VARCHAR(50),                             -- Unique merchant identifier
    merchant_category   VARCHAR(50),                             -- MCC (Merchant Category Code) group
    txn_country         CHAR(2),                                 -- Country where transaction occurred
    is_fraud            BOOLEAN NOT NULL DEFAULT FALSE,          -- Ground truth label
    fraud_probability   DECIMAL(5,4),                            -- ML model score (0.0000 - 1.0000)
    processing_time_ms  INT,                                     -- How long the transaction took to process
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ────────────────────────────────────────────────────────────────────────────
-- 1.4 PERFORMANCE INDEXES
-- Strategic indexing is critical for sub-second query performance on
-- large datasets. Each index targets a specific query pattern.
-- ────────────────────────────────────────────────────────────────────────────

-- Composite index: Most queries filter by card + time range
CREATE INDEX idx_txn_card_time 
    ON fact_transactions(card_id, txn_timestamp);

-- Partial index: Only index fraud transactions (much smaller, faster scans)
CREATE INDEX idx_txn_fraud 
    ON fact_transactions(is_fraud) 
    WHERE is_fraud = TRUE;

-- B-tree index: For amount-based range queries and percentile calculations
CREATE INDEX idx_txn_amount 
    ON fact_transactions(txn_amount);

-- Composite index: For merchant category risk analysis queries
CREATE INDEX idx_txn_merchant 
    ON fact_transactions(merchant_category, is_fraud);

-- BRIN index: Block Range Index for time-series data
-- BRIN is ~100x smaller than B-tree for sequential/time-ordered data.
-- Perfect for our timestamp column since transactions arrive chronologically.
CREATE INDEX idx_txn_timestamp 
    ON fact_transactions USING BRIN(txn_timestamp);


-- ============================================================================
-- SECTION 2: ADVANCED ANALYTICAL QUERIES
-- ============================================================================


-- ────────────────────────────────────────────────────────────────────────────
-- 2.1 MULTI-WINDOW TRANSACTION VELOCITY & BURST DETECTION
-- ────────────────────────────────────────────────────────────────────────────
-- PURPOSE: Detect accounts exhibiting abnormal transaction frequency or
--          spending patterns compared to their historical baseline.
--
-- TECHNIQUES USED:
--   • Multiple window functions with different frame specifications
--   • RANGE frame (time-based) vs ROWS frame (count-based)
--   • LAG() for inter-transaction timing
--   • PERCENT_RANK() for statistical percentile positioning
--   • Composite scoring with CASE expressions
--
-- WHY THIS MATTERS:
--   Fraudsters often "test" stolen cards with small transactions, then 
--   rapidly make large purchases. This query detects that burst pattern.
--   JP Morgan's real-time fraud monitoring system uses similar velocity
--   analysis on 2.5+ billion daily transactions.
-- ────────────────────────────────────────────────────────────────────────────

WITH velocity_metrics AS (
    SELECT
        t.card_id,
        t.txn_id,
        t.txn_timestamp,
        t.txn_amount,
        t.is_fraud,

        -- WINDOW 1: Count of transactions in rolling 1-hour window
        -- RANGE frame uses actual timestamp values (not row positions)
        -- This captures transactions within 60 minutes of current row
        COUNT(*) OVER (
            PARTITION BY t.card_id
            ORDER BY t.txn_timestamp
            RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW
        ) AS txn_count_1h,

        -- WINDOW 2: Cumulative spend in rolling 24-hour window
        -- Tracks total card spend over the past day
        SUM(t.txn_amount) OVER (
            PARTITION BY t.card_id
            ORDER BY t.txn_timestamp
            RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW
        ) AS spend_24h,

        -- WINDOW 3: Time gap from previous transaction (in seconds)
        -- LAG() retrieves the value from the previous row in the partition
        -- Extremely short gaps indicate automated/bot-driven transactions
        EXTRACT(EPOCH FROM (
            t.txn_timestamp - LAG(t.txn_timestamp) OVER (
                PARTITION BY t.card_id ORDER BY t.txn_timestamp)
        )) AS seconds_since_last_txn,

        -- WINDOW 4: Running average amount per card (excluding current txn)
        -- ROWS frame with UNBOUNDED PRECEDING to 1 PRECEDING gives us
        -- the historical average BEFORE this transaction
        AVG(t.txn_amount) OVER (
            PARTITION BY t.card_id
            ORDER BY t.txn_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS historical_avg_amount,

        -- WINDOW 5: Percentile rank of this transaction's amount
        -- among all transactions for this card. Value of 0.95 means
        -- this amount is larger than 95% of the card's other transactions.
        PERCENT_RANK() OVER (
            PARTITION BY t.card_id ORDER BY t.txn_amount
        ) AS amount_percentile

    FROM fact_transactions t
),

-- Second CTE: Score each transaction based on the velocity metrics above
anomaly_scored AS (
    SELECT
        *,
        -- COMPOSITE ANOMALY SCORE (0-100)
        -- Each component contributes to the overall risk:
        --   • High transaction count in 1h:     up to 30 points
        --   • Very short inter-transaction gap:  25 points
        --   • Amount far above historical avg:   25 points
        --   • Amount in top 5th percentile:      20 points
        LEAST(100, (
            CASE WHEN txn_count_1h > 5 THEN 30 ELSE txn_count_1h * 6 END
            + CASE WHEN seconds_since_last_txn < 60 THEN 25 ELSE 0 END
            + CASE WHEN txn_amount > COALESCE(historical_avg_amount, 0) * 3
                   THEN 25 ELSE 0 END
            + CASE WHEN amount_percentile > 0.95 THEN 20 ELSE 0 END
        )) AS velocity_risk_score,

        -- Human-readable velocity pattern classification
        CASE
            WHEN txn_count_1h >= 8 THEN 'CRITICAL_BURST'
            WHEN txn_count_1h >= 5 THEN 'HIGH_VELOCITY'
            WHEN txn_count_1h >= 3
                 AND seconds_since_last_txn < 120 THEN 'RAPID_SUCCESSION'
            ELSE 'NORMAL'
        END AS velocity_pattern

    FROM velocity_metrics
)

-- Final output: Only show flagged transactions, enriched with pattern-level stats
SELECT
    card_id,
    txn_id,
    txn_timestamp,
    txn_amount,
    txn_count_1h,
    ROUND(spend_24h, 2) AS rolling_24h_spend,
    ROUND(seconds_since_last_txn, 0) AS gap_seconds,
    velocity_risk_score,
    velocity_pattern,
    is_fraud,
    -- What percentage of transactions with this pattern are actually fraud?
    -- This is the precision metric for each velocity pattern.
    ROUND(
        AVG(CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END) OVER (
            PARTITION BY velocity_pattern
        ), 4
    ) AS pattern_fraud_rate
FROM anomaly_scored
WHERE velocity_risk_score >= 50      -- Only show medium-high risk
ORDER BY velocity_risk_score DESC, txn_timestamp;


-- ────────────────────────────────────────────────────────────────────────────
-- 2.2 RECURSIVE CTE: FRAUD CHAIN & CASCADING FRAUD DETECTION
-- ────────────────────────────────────────────────────────────────────────────
-- PURPOSE: Trace fraud chains — sequences of connected suspicious 
--          transactions linked by shared cards, devices, or IP addresses.
--          This is the exact approach used in AML (Anti-Money Laundering)
--          investigations at major banks.
--
-- TECHNIQUES USED:
--   • WITH RECURSIVE for graph traversal
--   • ARRAY operations for cycle detection (prevent infinite loops)
--   • Multi-table JOINs within recursive member
--   • ARRAY_AGG for collecting connected entities
--
-- HOW RECURSIVE CTEs WORK:
--   1. Base case (anchor): Select seed fraud transactions
--   2. Recursive member: Find transactions connected to seeds via 
--      shared device/IP within 48 hours
--   3. Termination: Stop when max_depth reached or no new connections
-- ────────────────────────────────────────────────────────────────────────────

WITH RECURSIVE fraud_seeds AS (
    -- ── BASE CASE: Start from confirmed fraud transactions ──
    -- These are our "seed" nodes in the fraud network graph
    SELECT
        t.txn_id,
        t.card_id,
        i.device_info,
        i.ip_address,
        t.txn_timestamp,
        t.txn_amount,
        1 AS chain_depth,                               -- Depth counter (starts at 1)
        ARRAY[t.txn_id] AS chain_path,                  -- Track visited nodes (cycle prevention)
        t.txn_amount AS chain_total                      -- Running total of chain exposure
    FROM fact_transactions t
    JOIN dim_identity i ON t.identity_id = i.identity_id
    WHERE t.is_fraud = TRUE                              -- Only confirmed fraud as seeds
      AND t.txn_timestamp >= CURRENT_DATE - INTERVAL '90 days'  -- Last 90 days
),

fraud_chain AS (
    SELECT * FROM fraud_seeds

    UNION ALL

    -- ── RECURSIVE MEMBER: Find connected transactions ──
    -- A transaction is "connected" if it shares the same device fingerprint
    -- OR the same IP address, AND occurred within 48 hours of the seed.
    SELECT
        t.txn_id,
        t.card_id,
        i.device_info,
        i.ip_address,
        t.txn_timestamp,
        t.txn_amount,
        fc.chain_depth + 1,                             -- Increment depth
        fc.chain_path || t.txn_id,                      -- Append to visited path
        fc.chain_total + t.txn_amount                   -- Accumulate chain exposure
    FROM fact_transactions t
    JOIN dim_identity i ON t.identity_id = i.identity_id
    JOIN fraud_chain fc ON (
        -- Connection criteria: same device OR same IP
        (i.device_info = fc.device_info
         OR i.ip_address = fc.ip_address)
        -- Cycle prevention: don't revisit transactions already in the chain
        AND t.txn_id != ALL(fc.chain_path)
        -- Temporal constraint: must be within 48 hours of previous link
        AND t.txn_timestamp BETWEEN
            fc.txn_timestamp AND
            fc.txn_timestamp + INTERVAL '48 hours'
    )
    WHERE fc.chain_depth < 10                            -- Safety: max recursion depth
),

-- Summarize each fraud chain by its seed transaction
chain_summary AS (
    SELECT
        chain_path[1] AS seed_txn_id,                    -- First txn in chain
        MAX(chain_depth) AS max_chain_length,
        COUNT(DISTINCT card_id) AS unique_cards_affected,
        COUNT(DISTINCT device_info) AS unique_devices,
        SUM(DISTINCT txn_amount) AS total_chain_exposure,
        MIN(txn_timestamp) AS chain_start,
        MAX(txn_timestamp) AS chain_end,
        ARRAY_AGG(DISTINCT card_id) AS affected_cards    -- Collect all affected card IDs
    FROM fraud_chain
    GROUP BY chain_path[1]
)

SELECT
    seed_txn_id,
    max_chain_length,
    unique_cards_affected,
    unique_devices,
    ROUND(total_chain_exposure, 2) AS total_exposure_usd,
    chain_end - chain_start AS chain_duration,
    -- Classify the chain based on scale and complexity
    CASE
        WHEN unique_cards_affected >= 10
             AND total_chain_exposure > 50000 THEN 'ORGANIZED_RING'      -- Large fraud ring
        WHEN unique_cards_affected >= 5 THEN 'MULTI_CARD_FRAUD'          -- Multiple stolen cards
        WHEN max_chain_length >= 5 THEN 'DEEP_CHAIN'                     -- Long chain of connected txns
        ELSE 'ISOLATED_CLUSTER'                                          -- Small cluster
    END AS chain_classification
FROM chain_summary
WHERE max_chain_length >= 3                              -- Only show chains with 3+ links
ORDER BY total_chain_exposure DESC
LIMIT 50;


-- ────────────────────────────────────────────────────────────────────────────
-- 2.3 STORED PROCEDURE: DYNAMIC FRAUD RISK SCORING ENGINE
-- ────────────────────────────────────────────────────────────────────────────
-- PURPOSE: Production-grade function that calculates a composite fraud
--          risk score for any incoming transaction in real-time.
--          Returns score, risk level, risk factors JSON, and recommended action.
--
-- TECHNIQUES USED:
--   • PL/pgSQL stored function with multiple OUT parameters
--   • JSONB output for structured risk factor audit trail
--   • Exception handling with SQLSTATE error codes
--   • Dynamic threshold calculation from historical data
--   • Z-score statistical anomaly detection
--
-- DESIGN PHILOSOPHY:
--   This function mimics the scoring engines at major banks:
--   - 5 independent risk components each contribute to the total
--   - Each component has a maximum cap to prevent single-factor dominance
--   - JSONB output enables downstream dashboards and audit logging
--   - Exception handling ensures the system never crashes on bad input
-- ────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION fraud_analytics.calculate_risk_score(
    p_card_id       INT,            -- Which card is being used
    p_txn_amount    DECIMAL,        -- How much is the transaction
    p_merchant_cat  VARCHAR,        -- What merchant category
    p_device_info   VARCHAR,        -- Device fingerprint
    p_txn_country   CHAR(2),        -- Where is the transaction happening
    OUT risk_score          DECIMAL,     -- Composite risk score (0-100)
    OUT risk_level          VARCHAR,     -- CRITICAL / HIGH / MEDIUM / LOW / MINIMAL
    OUT risk_factors        JSONB,       -- Detailed breakdown for audit
    OUT recommended_action  VARCHAR      -- BLOCK / 2FA / FLAG / APPROVE
)
RETURNS RECORD
LANGUAGE plpgsql
AS $$
DECLARE
    -- Component scores (each capped at a maximum)
    v_velocity_score        DECIMAL := 0;    -- Max 25 points
    v_amount_score          DECIMAL := 0;    -- Max 25 points
    v_merchant_score        DECIMAL := 0;    -- Max 20 points
    v_geo_score             DECIMAL := 0;    -- Max 15 points
    v_device_score          DECIMAL := 0;    -- Max 15 points

    -- Intermediate calculation variables
    v_historical_avg        DECIMAL;
    v_historical_std        DECIMAL;
    v_txn_count_1h          INT;
    v_merchant_fraud_rate   DECIMAL;
    v_last_txn_country      CHAR(2);
    v_last_txn_time         TIMESTAMP;
    v_device_fraud_count    INT;
BEGIN
    -- ── COMPONENT 1: VELOCITY SCORE (0-25) ──
    -- How many transactions has this card made in the last hour?
    -- Normal: 1-2, Suspicious: 3-4, High Risk: 5+
    SELECT COUNT(*)
    INTO v_txn_count_1h
    FROM fact_transactions
    WHERE card_id = p_card_id
      AND txn_timestamp >= NOW() - INTERVAL '1 hour';

    v_velocity_score := LEAST(25, v_txn_count_1h * 5);

    -- ── COMPONENT 2: AMOUNT ANOMALY SCORE (0-25) ──
    -- Z-score: How many standard deviations is this amount from 
    -- the card's 90-day historical average?
    -- Z > 3 is highly anomalous (99.7th percentile)
    SELECT AVG(txn_amount), STDDEV(txn_amount)
    INTO v_historical_avg, v_historical_std
    FROM fact_transactions
    WHERE card_id = p_card_id
      AND txn_timestamp >= NOW() - INTERVAL '90 days';

    IF v_historical_std > 0 THEN
        -- Scale z-score to 0-25 range (z=3 → ~24 points)
        v_amount_score := LEAST(25,
            GREATEST(0, (p_txn_amount - v_historical_avg) 
                        / v_historical_std * 8));
    END IF;

    -- ── COMPONENT 3: MERCHANT CATEGORY RISK (0-20) ──
    -- What's the fraud rate for this merchant category in the last 30 days?
    -- High-risk categories: online gambling, cryptocurrency, gift cards
    SELECT COALESCE(
        AVG(CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END), 0)
    INTO v_merchant_fraud_rate
    FROM fact_transactions
    WHERE merchant_category = p_merchant_cat
      AND txn_timestamp >= NOW() - INTERVAL '30 days';

    v_merchant_score := LEAST(20, v_merchant_fraud_rate * 200);

    -- ── COMPONENT 4: GEOGRAPHIC RISK (0-15) ──
    -- "Impossible travel": Was the card used in a different country
    -- within the last 2 hours? If yes, maximum geographic risk.
    SELECT txn_country, txn_timestamp
    INTO v_last_txn_country, v_last_txn_time
    FROM fact_transactions
    WHERE card_id = p_card_id
    ORDER BY txn_timestamp DESC LIMIT 1;

    IF v_last_txn_country IS DISTINCT FROM p_txn_country
       AND v_last_txn_time > NOW() - INTERVAL '2 hours' THEN
        v_geo_score := 15;  -- Impossible travel detected
    END IF;

    -- ── COMPONENT 5: DEVICE RISK (0-15) ──
    -- Has this device been associated with fraud before?
    -- Devices used in past fraud are extremely high risk.
    SELECT COUNT(*)
    INTO v_device_fraud_count
    FROM fact_transactions t
    JOIN dim_identity i ON t.identity_id = i.identity_id
    WHERE i.device_info = p_device_info
      AND t.is_fraud = TRUE;

    v_device_score := LEAST(15, v_device_fraud_count * 3);

    -- ── COMPOSITE SCORE CALCULATION ──
    risk_score := v_velocity_score + v_amount_score 
                  + v_merchant_score + v_geo_score + v_device_score;

    -- ── RISK LEVEL CLASSIFICATION ──
    risk_level := CASE
        WHEN risk_score >= 80 THEN 'CRITICAL'
        WHEN risk_score >= 60 THEN 'HIGH'
        WHEN risk_score >= 40 THEN 'MEDIUM'
        WHEN risk_score >= 20 THEN 'LOW'
        ELSE 'MINIMAL'
    END;

    -- ── RECOMMENDED ACTION ──
    -- Maps risk levels to business actions (configurable thresholds)
    recommended_action := CASE
        WHEN risk_score >= 80 THEN 'BLOCK_TRANSACTION'
        WHEN risk_score >= 60 THEN 'REQUIRE_2FA_AND_REVIEW'
        WHEN risk_score >= 40 THEN 'FLAG_FOR_REVIEW'
        ELSE 'APPROVE'
    END;

    -- ── BUILD RISK FACTORS JSON (Audit Trail) ──
    -- This JSONB output enables:
    --   1. Dashboard visualizations of risk components
    --   2. Regulatory audit compliance
    --   3. ML model training features
    risk_factors := jsonb_build_object(
        'velocity', v_velocity_score,
        'amount_anomaly', v_amount_score,
        'merchant_risk', v_merchant_score,
        'geo_risk', v_geo_score,
        'device_risk', v_device_score,
        'txn_count_1h', v_txn_count_1h,
        'historical_avg', ROUND(v_historical_avg, 2),
        'z_score', CASE WHEN v_historical_std > 0
            THEN ROUND((p_txn_amount - v_historical_avg) 
                       / v_historical_std, 2)
            ELSE NULL END
    );

-- ── ERROR HANDLING ──
-- If anything goes wrong, return safe defaults and log the error
EXCEPTION WHEN OTHERS THEN
    risk_score := -1;
    risk_level := 'ERROR';
    risk_factors := jsonb_build_object('error', SQLERRM);
    recommended_action := 'MANUAL_REVIEW';
END;
$$;

-- Example usage:
-- SELECT * FROM fraud_analytics.calculate_risk_score(
--     12345,          -- card_id
--     2999.99,        -- txn_amount
--     'electronics',  -- merchant_category
--     'iPhone14_A2B', -- device_info
--     'US'            -- txn_country
-- );


-- ────────────────────────────────────────────────────────────────────────────
-- 2.4 MATERIALIZED VIEW: REAL-TIME MERCHANT RISK DASHBOARD
-- ────────────────────────────────────────────────────────────────────────────
-- PURPOSE: Pre-compute merchant-level risk metrics for sub-second 
--          dashboard queries. Refreshed every 15 minutes via scheduler.
--
-- TECHNIQUES USED:
--   • Materialized View for query pre-computation
--   • FILTER clause for conditional aggregation (PostgreSQL-specific)
--   • PERCENTILE_CONT for median/P95 calculations
--   • NTILE() for decile-based risk tiering
--   • DENSE_RANK() for within-category ranking
--   • Z-score calculation using window AVG/STDDEV
--   • CONCURRENT refresh for zero-downtime updates
--
-- BUSINESS CONTEXT:
--   American Express's merchant monitoring system uses a similar pattern:
--   pre-aggregated risk dashboards that analysts can query instantly,
--   rather than running expensive queries against the raw transaction table.
-- ────────────────────────────────────────────────────────────────────────────

CREATE MATERIALIZED VIEW mv_merchant_risk_dashboard AS
WITH merchant_stats AS (
    SELECT
        t.merchant_category,
        t.merchant_id,

        -- Volume metrics
        COUNT(*) AS total_txns,
        COUNT(*) FILTER (WHERE t.is_fraud) AS fraud_txns,          -- FILTER clause (PG-specific)
        SUM(t.txn_amount) AS total_volume,
        SUM(t.txn_amount) FILTER (WHERE t.is_fraud) AS fraud_volume,

        -- Statistical metrics
        AVG(t.txn_amount) AS avg_txn_amount,
        PERCENTILE_CONT(0.95) WITHIN GROUP 
            (ORDER BY t.txn_amount) AS p95_amount,                  -- 95th percentile transaction
        STDDEV(t.txn_amount) AS amount_volatility,

        -- Customer metrics
        COUNT(DISTINCT t.card_id) AS unique_cards,
        COUNT(DISTINCT t.card_id) FILTER (WHERE t.is_fraud) AS fraud_cards,

        -- Temporal pattern
        AVG(EXTRACT(HOUR FROM t.txn_timestamp)) AS avg_hour         -- Average transaction hour

    FROM fact_transactions t
    WHERE t.txn_timestamp >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY t.merchant_category, t.merchant_id
),

ranked AS (
    SELECT
        *,
        -- Fraud rate calculations
        ROUND(fraud_txns::DECIMAL / NULLIF(total_txns, 0) * 100, 3) 
            AS fraud_rate_pct,
        ROUND(fraud_volume / NULLIF(total_volume, 0) * 100, 3) 
            AS fraud_volume_pct,

        -- NTILE(10): Divide all merchants into 10 equal groups by fraud rate
        -- Decile 10 = top 10% highest fraud rate merchants
        NTILE(10) OVER (
            ORDER BY fraud_txns::DECIMAL / NULLIF(total_txns, 0)
        ) AS fraud_rate_decile,

        -- DENSE_RANK: Rank merchants within their category by fraud volume
        -- Unlike RANK(), DENSE_RANK() has no gaps (1,2,3 not 1,2,4)
        DENSE_RANK() OVER (
            PARTITION BY merchant_category
            ORDER BY fraud_volume DESC
        ) AS category_fraud_rank,

        -- Z-SCORE: How many std deviations is this merchant's fraud rate
        -- from its category average? Z > 2.0 = statistically significant outlier
        (fraud_txns::DECIMAL / NULLIF(total_txns, 0)
         - AVG(fraud_txns::DECIMAL / NULLIF(total_txns, 0)) 
             OVER (PARTITION BY merchant_category))
        / NULLIF(STDDEV(fraud_txns::DECIMAL / NULLIF(total_txns, 0)) 
             OVER (PARTITION BY merchant_category), 0)
            AS fraud_rate_zscore

    FROM merchant_stats
    WHERE total_txns >= 100                              -- Minimum sample for statistical significance
)

SELECT
    *,
    -- Final risk tier assignment using multiple criteria
    CASE
        WHEN fraud_rate_decile >= 9 
             AND fraud_rate_zscore > 2.0 THEN 'CRITICAL_RISK'       -- Top decile AND statistical outlier
        WHEN fraud_rate_decile >= 8 THEN 'HIGH_RISK'                -- Top 20% fraud rate
        WHEN fraud_rate_decile >= 6 THEN 'ELEVATED'                 -- Above median fraud rate
        ELSE 'NORMAL'
    END AS merchant_risk_tier
FROM ranked;

-- Unique index enables CONCURRENTLY refresh (no locks during refresh)
CREATE UNIQUE INDEX idx_mv_merchant 
    ON mv_merchant_risk_dashboard(merchant_id);

-- To refresh (run via pg_cron or application scheduler every 15 min):
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_merchant_risk_dashboard;


-- ────────────────────────────────────────────────────────────────────────────
-- 2.5 DYNAMIC PIVOT: TIME-SERIES FRAUD HEATMAP
-- ────────────────────────────────────────────────────────────────────────────
-- PURPOSE: Generate a fraud rate heatmap by hour-of-day × day-of-week.
--          Reveals temporal patterns (e.g., fraud spikes at 3 AM on weekends).
--
-- TECHNIQUES USED:
--   • Conditional aggregation to pivot rows into columns
--   • EXTRACT() for temporal decomposition
--   • Cross-tab pattern without CROSSTAB extension
--
-- BUSINESS CONTEXT:
--   This heatmap is used for:
--   1. Fraud rule engine tuning (tighter thresholds during high-risk hours)
--   2. Analyst shift scheduling (more analysts during peak fraud windows)
--   3. Card blocking policies (auto-block transactions at 3 AM for certain cards)
-- ────────────────────────────────────────────────────────────────────────────

SELECT
    EXTRACT(HOUR FROM txn_timestamp)::INT AS hour_of_day,

    -- Each column = one day of the week, value = fraud rate %
    ROUND(AVG(CASE WHEN EXTRACT(DOW FROM txn_timestamp) = 0
        THEN CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END END) * 100, 2) AS sun_fraud_pct,
    ROUND(AVG(CASE WHEN EXTRACT(DOW FROM txn_timestamp) = 1
        THEN CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END END) * 100, 2) AS mon_fraud_pct,
    ROUND(AVG(CASE WHEN EXTRACT(DOW FROM txn_timestamp) = 2
        THEN CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END END) * 100, 2) AS tue_fraud_pct,
    ROUND(AVG(CASE WHEN EXTRACT(DOW FROM txn_timestamp) = 3
        THEN CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END END) * 100, 2) AS wed_fraud_pct,
    ROUND(AVG(CASE WHEN EXTRACT(DOW FROM txn_timestamp) = 4
        THEN CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END END) * 100, 2) AS thu_fraud_pct,
    ROUND(AVG(CASE WHEN EXTRACT(DOW FROM txn_timestamp) = 5
        THEN CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END END) * 100, 2) AS fri_fraud_pct,
    ROUND(AVG(CASE WHEN EXTRACT(DOW FROM txn_timestamp) = 6
        THEN CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END END) * 100, 2) AS sat_fraud_pct,

    -- Overall fraud rate for this hour (across all days)
    ROUND(AVG(CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END) * 100, 2) AS overall_fraud_pct,

    -- Total transaction volume per hour (context for the rates above)
    COUNT(*) AS total_transactions

FROM fact_transactions
WHERE txn_timestamp >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY EXTRACT(HOUR FROM txn_timestamp)
ORDER BY hour_of_day;


-- ============================================================================
-- SECTION 3: UTILITY QUERIES
-- ============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- 3.1 PORTFOLIO-LEVEL FRAUD KPI SUMMARY
-- Quick executive dashboard: key fraud metrics at a glance
-- ────────────────────────────────────────────────────────────────────────────
SELECT
    COUNT(*) AS total_transactions,
    COUNT(*) FILTER (WHERE is_fraud) AS fraud_transactions,
    ROUND(AVG(CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END) * 100, 3) AS fraud_rate_pct,
    ROUND(SUM(txn_amount), 2) AS total_volume,
    ROUND(SUM(txn_amount) FILTER (WHERE is_fraud), 2) AS fraud_volume,
    ROUND(AVG(txn_amount), 2) AS avg_txn_amount,
    ROUND(AVG(txn_amount) FILTER (WHERE is_fraud), 2) AS avg_fraud_amount,
    COUNT(DISTINCT card_id) AS unique_cards,
    COUNT(DISTINCT card_id) FILTER (WHERE is_fraud) AS compromised_cards
FROM fact_transactions
WHERE txn_timestamp >= CURRENT_DATE - INTERVAL '30 days';


-- ────────────────────────────────────────────────────────────────────────────
-- 3.2 ROLLING 7-DAY FRAUD TREND
-- Daily fraud metrics with 7-day moving average for trend analysis
-- ────────────────────────────────────────────────────────────────────────────
WITH daily AS (
    SELECT
        txn_timestamp::DATE AS txn_date,
        COUNT(*) AS daily_txns,
        COUNT(*) FILTER (WHERE is_fraud) AS daily_fraud,
        ROUND(AVG(CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END) * 100, 3) AS daily_fraud_rate
    FROM fact_transactions
    WHERE txn_timestamp >= CURRENT_DATE - INTERVAL '60 days'
    GROUP BY txn_timestamp::DATE
)
SELECT
    txn_date,
    daily_txns,
    daily_fraud,
    daily_fraud_rate,
    -- 7-day moving average smooths daily volatility
    ROUND(AVG(daily_fraud_rate) OVER (
        ORDER BY txn_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 3) AS ma_7d_fraud_rate
FROM daily
ORDER BY txn_date;


-- ────────────────────────────────────────────────────────────────────────────
-- 3.3 CARD-LEVEL RISK PROFILE
-- Generate a comprehensive risk profile for any individual card
-- Useful for fraud investigation and customer service teams
-- ────────────────────────────────────────────────────────────────────────────
SELECT
    c.card_id,
    c.card_type,
    c.card_network,
    c.issuing_bank,
    c.bin_country,
    c.risk_tier,
    COUNT(t.txn_id) AS total_txns,
    COUNT(t.txn_id) FILTER (WHERE t.is_fraud) AS fraud_txns,
    ROUND(SUM(t.txn_amount), 2) AS total_spend,
    ROUND(AVG(t.txn_amount), 2) AS avg_txn,
    ROUND(STDDEV(t.txn_amount), 2) AS txn_amount_stddev,
    MIN(t.txn_timestamp) AS first_txn,
    MAX(t.txn_timestamp) AS last_txn,
    COUNT(DISTINCT t.txn_country) AS countries_used,
    COUNT(DISTINCT t.merchant_category) AS merchant_categories
FROM dim_cards c
LEFT JOIN fact_transactions t ON c.card_id = t.card_id
GROUP BY c.card_id, c.card_type, c.card_network, 
         c.issuing_bank, c.bin_country, c.risk_tier
ORDER BY fraud_txns DESC
LIMIT 100;


-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║  HOW TO RUN THIS PROJECT                                               ║
-- ║  1. Download IEEE-CIS Fraud Detection dataset from Kaggle:             ║
-- ║     https://www.kaggle.com/c/ieee-fraud-detection                      ║
-- ║  2. Install PostgreSQL 16+ and create a database                       ║
-- ║  3. Run this entire SQL file to create schema + all objects            ║
-- ║  4. Load CSV data: COPY fact_transactions FROM '/path/to/data.csv'    ║
-- ║     WITH (FORMAT csv, HEADER true);                                    ║
-- ║  5. Execute queries individually to analyze results                    ║
-- ╚══════════════════════════════════════════════════════════════════════════╝
