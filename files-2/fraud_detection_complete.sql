-- ╔══════════════════════════════════════════════════════════════════════════════╗
-- ║  CREDIT CARD FRAUD DETECTION & TRANSACTION ANOMALY ANALYTICS               ║
-- ║  Engine: PostgreSQL 16+ | Advanced SQL Analytics                           ║
-- ║  Dataset: IEEE-CIS Fraud Detection (Kaggle) - 590,540 Transactions        ║
-- ║  Author: Aditi Datt (AD9319)                                               ║
-- ║  GitHub: https://github.com/AD9319/fraud-detection                         ║
-- ║  Date: February 2026                                                       ║
-- ║  Portfolio Project: Financial Services Analytics                           ║
-- ╚══════════════════════════════════════════════════════════════════════════════╝

-- ============================================================================
-- TABLE OF CONTENTS
-- ============================================================================
-- 1. DATABASE SCHEMA DESIGN (Star Schema)
--    1.1 Drop existing schema (fresh start)
--    1.2 dim_cards         - Card dimension table
--    1.3 dim_identity      - Device & identity fingerprint dimension
--    1.4 fact_transactions  - Core transaction fact table
--    1.5 Performance Indexes
--
-- 2. DATA INGESTION SECTION
--    2.1 Create staging table for raw CSV data
--    2.2 Load raw data from CSV
--    2.3 Transform & populate dimension tables
--    2.4 Populate fact table
--    2.5 Data quality validation
--
-- 3. ADVANCED ANALYTICAL QUERIES
--    3.1 Multi-Window Transaction Velocity & Burst Detection
--    3.2 Recursive CTE: Fraud Chain & Cascading Fraud Detection
--    3.3 Stored Procedure: Dynamic Risk Scoring Engine
--    3.4 Materialized View: Real-Time Merchant Risk Dashboard
--    3.5 Dynamic Pivot: Time-Series Fraud Heatmap
--
-- 4. UTILITY QUERIES
--    4.1 Portfolio-Level Fraud KPI Summary
--    4.2 Rolling 7-Day Fraud Trend
--    4.3 Card-Level Risk Profile
-- ============================================================================


-- ============================================================================
-- SECTION 1: DATABASE SCHEMA DESIGN
-- Architecture: Star Schema optimized for OLAP analytical workloads
-- The fact table (transactions) sits at the center, with dimension tables
-- for cards and identity/device information branching out.
-- ============================================================================

-- 1.1 DROP EXISTING SCHEMA (FRESH START)
-- Uncomment if you want to start fresh
-- DROP SCHEMA IF EXISTS fraud_analytics CASCADE;

CREATE SCHEMA IF NOT EXISTS fraud_analytics;
SET search_path TO fraud_analytics;

-- ────────────────────────────────────────────────────────────────────────────
-- 1.2 DIMENSION TABLE: Card Information
-- Stores static card attributes. Each card has a unique hash, type,
-- issuing bank, and a dynamic risk_tier that can be updated as new
-- fraud intelligence becomes available.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_cards (
    card_id         SERIAL PRIMARY KEY,
    card_hash       VARCHAR(64) NOT NULL UNIQUE,                   -- Tokenized card number (PCI compliant)
    card_type       VARCHAR(20) CHECK (card_type IN 
                        ('credit', 'debit', 'mastercard', 'visa', 'amex', 'unknown')),
    card_network    VARCHAR(30),                                 -- Visa, Mastercard, Amex, etc.
    issuing_bank    VARCHAR(100),                                -- Bank that issued the card
    bin_country     CHAR(2),                                     -- Country from BIN lookup (ISO 3166)
    enrollment_date DATE,                                        -- When the card was first enrolled
    risk_tier       SMALLINT DEFAULT 1 
                        CHECK (risk_tier BETWEEN 1 AND 5),       -- 1=Low, 5=Critical risk
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ────────────────────────────────────────────────────────────────────────────
-- 1.3 DIMENSION TABLE: Device & Identity Fingerprints
-- Captures the digital footprint of each transaction session.
-- Device fingerprinting is a key fraud signal — the same device used
-- across multiple stolen cards is a strong indicator of organized fraud.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_identity (
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
    is_proxy        BOOLEAN DEFAULT FALSE,                       -- Whether IP is a known proxy/VPN
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(device_info, ip_address, email_domain)               -- Composite unique constraint
);

-- ────────────────────────────────────────────────────────────────────────────
-- 1.4 FACT TABLE: Transactions
-- The core analytical table. Every row represents one transaction event.
-- This table is designed for high-volume analytical queries with 
-- appropriate data types and constraints for data integrity.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_transactions (
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
    fraud_probability   DECIMAL(5,4) CHECK (fraud_probability BETWEEN 0.0000 AND 1.0000),
    processing_time_ms  INT,                                     -- How long the transaction took to process
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ────────────────────────────────────────────────────────────────────────────
-- 1.5 PERFORMANCE INDEXES
-- Strategic indexing is critical for sub-second query performance on
-- large datasets. Each index targets a specific query pattern.
-- ────────────────────────────────────────────────────────────────────────────

-- Composite index: Most queries filter by card + time range
CREATE INDEX IF NOT EXISTS idx_txn_card_time 
    ON fact_transactions(card_id, txn_timestamp);

-- Partial index: Only index fraud transactions (much smaller, faster scans)
CREATE INDEX IF NOT EXISTS idx_txn_fraud 
    ON fact_transactions(is_fraud) 
    WHERE is_fraud = TRUE;

-- B-tree index: For amount-based range queries and percentile calculations
CREATE INDEX IF NOT EXISTS idx_txn_amount 
    ON fact_transactions(txn_amount);

-- Composite index: For merchant category risk analysis queries
CREATE INDEX IF NOT EXISTS idx_txn_merchant 
    ON fact_transactions(merchant_category, is_fraud);

-- BRIN index: Block Range Index for time-series data
-- BRIN is ~100x smaller than B-tree for sequential/time-ordered data.
-- Perfect for our timestamp column since transactions arrive chronologically.
CREATE INDEX IF NOT EXISTS idx_txn_timestamp 
    ON fact_transactions USING BRIN(txn_timestamp);

-- Index for identity-based fraud detection (same device across multiple cards)
CREATE INDEX IF NOT EXISTS idx_txn_identity
    ON fact_transactions(identity_id, is_fraud);


-- ============================================================================
-- SECTION 2: DATA INGESTION & TRANSFORMATION
-- This section handles loading raw CSV data from your GitHub repo
-- and populating the star schema with clean, normalized data
-- ============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- 2.1 CREATE STAGING TABLE FOR RAW CSV DATA
-- This temporary table will hold data exactly as it appears in the CSV
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS stg_raw_transactions (
    TransactionID           BIGINT,
    TransactionTime         TIMESTAMP,
    TransactionAmount       DECIMAL(12,2),
    CardHash                VARCHAR(64),
    CardType                VARCHAR(20),
    CardNetwork             VARCHAR(30),
    IssuingBank             VARCHAR(100),
    DeviceType              VARCHAR(30),
    DeviceHash              VARCHAR(100),
    IPAddress               VARCHAR(15),
    EmailDomain             VARCHAR(100),
    Browser                 VARCHAR(50),
    OSType                  VARCHAR(50),
    ScreenResolution        VARCHAR(20),
    Latitude                DECIMAL(9,6),
    Longitude               DECIMAL(9,6),
    IsProxy                 BOOLEAN,
    ProductCategory         VARCHAR(50),
    MerchantID              VARCHAR(50),
    MerchantCategory        VARCHAR(50),
    TransactionCountry      CHAR(2),
    IsFraud                 BOOLEAN,
    FraudProbability        DECIMAL(5,4),
    ProcessingTime          INT
);

-- ────────────────────────────────────────────────────────────────────────────
-- 2.2 LOAD RAW DATA FROM CSV
-- OPTION A: Load from local file path
-- OPTION B: Load from GitHub raw CSV URL (via external data wrapper)
-- ────────────────────────────────────────────────────────────────────────────

-- OPTION A: LOAD FROM LOCAL FILE (Uncomment if CSV is on your machine)
-- COPY stg_raw_transactions FROM '/path/to/your/transactions.csv'
--     WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

-- OPTION B: LOAD FROM GITHUB (Recommended for portfolio projects)
-- First, create a wrapper to fetch from GitHub, then parse the CSV
-- You can download the CSV and reference it locally, or use this approach:

-- Example: If your GitHub repo has the CSV at:
-- https://raw.githubusercontent.com/AD9319/fraud-detection/main/transactions.csv

-- For PostgreSQL, you can:
-- 1. Download manually: wget https://raw.githubusercontent.com/AD9319/fraud-detection/main/transactions.csv
-- 2. Then load with:
-- COPY stg_raw_transactions FROM '/path/to/downloaded/transactions.csv'
--     WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

-- OR use a Python script (psycopg2) to load from URL directly
-- See section below for Python loader script

-- ────────────────────────────────────────────────────────────────────────────
-- 2.3 POPULATE DIMENSION TABLES (CARDS)
-- Extract unique cards from staging table and insert into dim_cards
-- ────────────────────────────────────────────────────────────────────────────

-- Check if data exists before transforming
-- SELECT COUNT(*) as staging_record_count FROM stg_raw_transactions;

-- Populate dim_cards from unique card records
INSERT INTO dim_cards (card_hash, card_type, card_network, issuing_bank, bin_country, risk_tier)
SELECT DISTINCT
    stg.CardHash,
    COALESCE(LOWER(stg.CardType), 'unknown')::VARCHAR(20),
    stg.CardNetwork,
    stg.IssuingBank,
    stg.TransactionCountry,  -- Use transaction country as proxy for BIN country
    1  -- Default risk tier, can be updated later based on fraud patterns
FROM stg_raw_transactions stg
ON CONFLICT (card_hash) DO NOTHING;

-- ────────────────────────────────────────────────────────────────────────────
-- 2.4 POPULATE DIMENSION TABLES (IDENTITY/DEVICE)
-- Extract unique device fingerprints from staging table
-- ────────────────────────────────────────────────────────────────────────────

INSERT INTO dim_identity (device_type, device_info, ip_address, email_domain, 
                          browser, os_type, screen_res, geo_latitude, geo_longitude, is_proxy)
SELECT DISTINCT
    stg.DeviceType,
    stg.DeviceHash,
    stg.IPAddress::INET,
    stg.EmailDomain,
    stg.Browser,
    stg.OSType,
    stg.ScreenResolution,
    stg.Latitude,
    stg.Longitude,
    COALESCE(stg.IsProxy, FALSE)
FROM stg_raw_transactions stg
ON CONFLICT (device_info, ip_address, email_domain) DO NOTHING;

-- ────────────────────────────────────────────────────────────────────────────
-- 2.5 POPULATE FACT TABLE (TRANSACTIONS)
-- Join staging data with dimension tables and insert into fact_transactions
-- ────────────────────────────────────────────────────────────────────────────

INSERT INTO fact_transactions 
    (card_id, identity_id, txn_timestamp, txn_amount, product_category, 
     merchant_id, merchant_category, txn_country, is_fraud, fraud_probability, processing_time_ms)
SELECT
    dc.card_id,
    di.identity_id,
    stg.TransactionTime,
    stg.TransactionAmount,
    stg.ProductCategory,
    stg.MerchantID,
    stg.MerchantCategory,
    stg.TransactionCountry,
    stg.IsFraud,
    stg.FraudProbability,
    stg.ProcessingTime
FROM stg_raw_transactions stg
INNER JOIN dim_cards dc ON stg.CardHash = dc.card_hash
INNER JOIN dim_identity di ON stg.DeviceHash = di.device_info 
                              AND stg.IPAddress::INET = di.ip_address
                              AND stg.EmailDomain = di.email_domain
ON CONFLICT DO NOTHING;

-- ────────────────────────────────────────────────────────────────────────────
-- 2.6 DATA QUALITY VALIDATION
-- Verify data integrity after load
-- ────────────────────────────────────────────────────────────────────────────

-- Check total record counts
SELECT 
    'Total Transactions Loaded' AS metric,
    COUNT(*) AS count
FROM fact_transactions
UNION ALL
SELECT 
    'Unique Cards',
    COUNT(DISTINCT card_id)
FROM fact_transactions
UNION ALL
SELECT 
    'Fraud Transactions',
    COUNT(*) FILTER (WHERE is_fraud = TRUE)
FROM fact_transactions
UNION ALL
SELECT 
    'Fraud Rate (%)',
    ROUND(COUNT(*) FILTER (WHERE is_fraud = TRUE)::DECIMAL / COUNT(*) * 100, 3)
FROM fact_transactions;

-- Validate data types and constraints
SELECT 
    'NULL check: txn_amount' AS validation,
    COUNT(*) AS null_count
FROM fact_transactions
WHERE txn_amount IS NULL
UNION ALL
SELECT 
    'NULL check: txn_timestamp',
    COUNT(*)
FROM fact_transactions
WHERE txn_timestamp IS NULL
UNION ALL
SELECT 
    'NULL check: is_fraud',
    COUNT(*)
FROM fact_transactions
WHERE is_fraud IS NULL;


-- ============================================================================
-- SECTION 3: ADVANCED ANALYTICAL QUERIES
-- Enterprise-grade fraud detection using advanced SQL techniques
-- ============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- 3.1 MULTI-WINDOW TRANSACTION VELOCITY & BURST DETECTION
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

CREATE OR REPLACE VIEW v_velocity_anomalies AS
WITH card_velocity AS (
    SELECT
        card_id,
        txn_id,
        txn_timestamp,
        txn_amount,
        is_fraud,
        -- Count transactions in a 1-hour rolling window
        COUNT(*) OVER (
            PARTITION BY card_id 
            ORDER BY txn_timestamp 
            RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW
        ) AS txn_count_1h,
        -- Sum amount in a 1-hour rolling window
        SUM(txn_amount) OVER (
            PARTITION BY card_id 
            ORDER BY txn_timestamp 
            RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW
        ) AS txn_sum_1h,
        -- Time since last transaction
        EXTRACT(EPOCH FROM txn_timestamp - LAG(txn_timestamp) OVER (
            PARTITION BY card_id ORDER BY txn_timestamp
        )) / 60.0 AS minutes_since_last_txn,
        -- Percentile rank of amount for this card
        PERCENT_RANK() OVER (
            PARTITION BY card_id 
            ORDER BY txn_amount
        ) AS amount_percentile
    FROM fact_transactions
    WHERE txn_timestamp >= CURRENT_DATE - INTERVAL '30 days'
),
velocity_scored AS (
    SELECT
        *,
        -- Composite velocity score
        CASE
            WHEN txn_count_1h >= 5 AND txn_sum_1h > 5000 THEN 'CRITICAL'
            WHEN txn_count_1h >= 3 AND txn_sum_1h > 2500 THEN 'HIGH'
            WHEN txn_count_1h >= 2 AND minutes_since_last_txn < 5 THEN 'MEDIUM'
            WHEN amount_percentile > 0.95 THEN 'ELEVATED'
            ELSE 'NORMAL'
        END AS velocity_risk_tier
    FROM card_velocity
)
SELECT
    card_id,
    txn_id,
    txn_timestamp,
    txn_amount,
    is_fraud,
    txn_count_1h,
    txn_sum_1h,
    minutes_since_last_txn,
    ROUND(amount_percentile * 100, 2) AS amount_percentile_rank,
    velocity_risk_tier
FROM velocity_scored
WHERE velocity_risk_tier IN ('CRITICAL', 'HIGH', 'MEDIUM')
ORDER BY txn_timestamp DESC;

-- View detail: Shows top velocity anomalies
-- SELECT * FROM v_velocity_anomalies LIMIT 50;


-- ────────────────────────────────────────────────────────────────────────────
-- 3.2 RECURSIVE CTE: FRAUD CHAIN & CASCADING FRAUD DETECTION
-- ────────────────────────────────────────────────────────────────────────────
-- PURPOSE: Detect fraud rings by identifying cards/devices connected through
--          shared attributes (same IP, same email, same device fingerprint).
--
-- HOW IT WORKS:
--   A single fraudulent transaction can expose an entire fraud ring if we
--   follow the chain of connected accounts. This recursive CTE starts with
--   known fraud cases and recursively finds all connected cards.
--
-- BUSINESS IMPACT:
--   One fraudster caught → Identify all 8-12 cloned cards they're using
--   Prevents cascading fraud losses
-- ────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW v_fraud_rings AS
WITH RECURSIVE fraud_chain AS (
    -- Base case: Known fraud transactions
    SELECT
        t.card_id,
        t.identity_id,
        t.txn_id,
        t.is_fraud,
        dc.card_hash,
        di.ip_address,
        di.email_domain,
        di.device_info,
        ARRAY[t.card_id] AS card_chain,
        1 AS chain_depth
    FROM fact_transactions t
    INNER JOIN dim_cards dc ON t.card_id = dc.card_id
    INNER JOIN dim_identity di ON t.identity_id = di.identity_id
    WHERE t.is_fraud = TRUE
    
    UNION ALL
    
    -- Recursive case: Find cards connected through shared attributes
    SELECT
        t.card_id,
        t.identity_id,
        t.txn_id,
        t.is_fraud,
        dc.card_hash,
        di.ip_address,
        di.email_domain,
        di.device_info,
        fc.card_chain || t.card_id,
        fc.chain_depth + 1
    FROM fraud_chain fc
    INNER JOIN fact_transactions t ON (
        -- Connect if they share IP address OR email domain OR device
        (t.identity_id IN (
            SELECT identity_id FROM dim_identity 
            WHERE ip_address = fc.ip_address
               OR email_domain = fc.email_domain
               OR device_info = fc.device_info
        ))
        AND t.card_id != fc.card_id  -- Avoid loops
        AND t.card_id != ALL(fc.card_chain)  -- Not already in chain
    )
    INNER JOIN dim_cards dc ON t.card_id = dc.card_id
    INNER JOIN dim_identity di ON t.identity_id = di.identity_id
    WHERE fc.chain_depth < 5  -- Limit recursion depth
)
SELECT
    card_chain[1] AS primary_fraudulent_card,
    COUNT(DISTINCT card_id) AS connected_cards_count,
    ARRAY_LENGTH(card_chain, 1) AS chain_length,
    COUNT(*) FILTER (WHERE is_fraud = TRUE) AS confirmed_fraud_txns,
    MAX(chain_depth) AS max_chain_depth,
    STRING_AGG(DISTINCT ip_address::TEXT, ', ') AS shared_ip_addresses,
    STRING_AGG(DISTINCT email_domain, ', ') AS shared_email_domains
FROM fraud_chain
GROUP BY card_chain[1]
ORDER BY connected_cards_count DESC;

-- View detail: Shows fraud rings and connected accounts
-- SELECT * FROM v_fraud_rings;


-- ────────────────────────────────────────────────────────────────────────────
-- 3.3 STORED PROCEDURE: DYNAMIC RISK SCORING ENGINE
-- ────────────────────────────────────────────────────────────────────────────
-- PURPOSE: Calculate comprehensive risk scores for transactions in real-time
--          using multiple signals (velocity, amount, merchant, geography, etc.)
--
-- SCORING MODEL:
--   Base Score = 10 points
--   + Velocity Risk: 0-30 points
--   + Amount Outlier: 0-20 points
--   + Merchant Risk: 0-15 points
--   + Geographic Risk: 0-15 points
--   + Device Risk: 0-10 points
--   Final Score: 10-100 (>70 = BLOCK, 50-70 = CHALLENGE, <50 = ALLOW)
-- ────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION calculate_transaction_risk_score(
    p_card_id INT,
    p_identity_id INT,
    p_txn_amount DECIMAL,
    p_merchant_category VARCHAR,
    p_txn_country CHAR(2)
)
RETURNS TABLE (
    risk_score INT,
    risk_level VARCHAR,
    velocity_component INT,
    amount_component INT,
    merchant_component INT,
    geographic_component INT,
    device_component INT,
    recommendation VARCHAR
) AS $$
DECLARE
    v_base_score INT := 10;
    v_velocity_score INT := 0;
    v_amount_score INT := 0;
    v_merchant_score INT := 0;
    v_geographic_score INT := 0;
    v_device_score INT := 0;
    v_total_score INT := 0;
    v_card_avg_amount DECIMAL;
    v_card_stddev DECIMAL;
    v_merchant_fraud_rate DECIMAL;
    v_card_countries INT;
    v_device_fraud_rate DECIMAL;
BEGIN
    -- VELOCITY COMPONENT: Recent transaction count for this card
    SELECT COUNT(*) INTO v_velocity_score
    FROM fact_transactions
    WHERE card_id = p_card_id
      AND txn_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 hour';
    
    v_velocity_score := LEAST(v_velocity_score * 5, 30);  -- Cap at 30
    
    -- AMOUNT COMPONENT: Is this amount an outlier for this card?
    SELECT AVG(txn_amount), STDDEV(txn_amount) INTO v_card_avg_amount, v_card_stddev
    FROM fact_transactions
    WHERE card_id = p_card_id
      AND txn_timestamp >= CURRENT_TIMESTAMP - INTERVAL '90 days';
    
    IF v_card_stddev > 0 AND p_txn_amount > (v_card_avg_amount + 2 * v_card_stddev) THEN
        v_amount_score := 20;  -- Outlier: assign max points
    ELSIF p_txn_amount > COALESCE(v_card_avg_amount, 0) * 2 THEN
        v_amount_score := 10;  -- 2x average
    END IF;
    
    -- MERCHANT COMPONENT: What's the fraud rate for this merchant?
    SELECT ROUND(COUNT(*) FILTER (WHERE is_fraud = TRUE)::DECIMAL / 
                 COUNT(*) * 100, 2) INTO v_merchant_fraud_rate
    FROM fact_transactions
    WHERE merchant_category = p_merchant_category
      AND txn_timestamp >= CURRENT_TIMESTAMP - INTERVAL '90 days';
    
    v_merchant_score := LEAST(ROUND(COALESCE(v_merchant_fraud_rate, 0))::INT, 15);
    
    -- GEOGRAPHIC COMPONENT: Is card being used in new countries?
    SELECT COUNT(DISTINCT txn_country) INTO v_card_countries
    FROM fact_transactions
    WHERE card_id = p_card_id;
    
    IF p_txn_country NOT IN (
        SELECT DISTINCT txn_country FROM fact_transactions 
        WHERE card_id = p_card_id AND txn_timestamp >= CURRENT_TIMESTAMP - INTERVAL '30 days'
    ) THEN
        v_geographic_score := 15;  -- New country: max points
    END IF;
    
    -- DEVICE COMPONENT: Fraud rate for this device
    SELECT ROUND(COUNT(*) FILTER (WHERE is_fraud = TRUE)::DECIMAL / 
                 COUNT(*) * 100, 2) INTO v_device_fraud_rate
    FROM fact_transactions
    WHERE identity_id = p_identity_id
      AND txn_timestamp >= CURRENT_TIMESTAMP - INTERVAL '90 days';
    
    v_device_score := LEAST(ROUND(COALESCE(v_device_fraud_rate, 0))::INT, 10);
    
    -- TOTAL SCORE
    v_total_score := v_base_score + v_velocity_score + v_amount_score + 
                     v_merchant_score + v_geographic_score + v_device_score;
    
    -- Ensure score is between 10 and 100
    v_total_score := GREATEST(10, LEAST(v_total_score, 100));
    
    RETURN QUERY SELECT
        v_total_score,
        CASE
            WHEN v_total_score >= 70 THEN 'BLOCK'
            WHEN v_total_score >= 50 THEN 'CHALLENGE'
            ELSE 'ALLOW'
        END,
        v_velocity_score,
        v_amount_score,
        v_merchant_score,
        v_geographic_score,
        v_device_score,
        CASE
            WHEN v_total_score >= 70 THEN 'Decline transaction & verify cardholder'
            WHEN v_total_score >= 50 THEN 'Request additional authentication (2FA, OTP)'
            ELSE 'Proceed with transaction'
        END;
END;
$$ LANGUAGE plpgsql STABLE;

-- Usage example:
-- SELECT * FROM calculate_transaction_risk_score(1, 1, 150.00, 'Electronics', 'US');


-- ────────────────────────────────────────────────────────────────────────────
-- 3.4 MATERIALIZED VIEW: REAL-TIME MERCHANT RISK DASHBOARD
-- ────────────────────────────────────────────────────────────────────────────
-- PURPOSE: Identify high-risk merchants that are disproportionately targeted
--          by fraudsters. This dashboard enables risk-based pricing and
--          enhanced monitoring.
--
-- BUSINESS CONTEXT:
--   Some merchant categories (dating apps, gift cards, crypto) have 5-10x
--   higher fraud rates. Flagging these enables targeted defenses.
-- ────────────────────────────────────────────────────────────────────────────

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_merchant_risk_dashboard AS
WITH merchant_stats AS (
    SELECT
        merchant_category,
        merchant_id,
        COUNT(*) AS total_txns,
        COUNT(*) FILTER (WHERE is_fraud = TRUE) AS fraud_txns,
        ROUND(COUNT(*) FILTER (WHERE is_fraud = TRUE)::DECIMAL / 
              COUNT(*) * 100, 2) AS fraud_rate_pct,
        ROUND(AVG(txn_amount), 2) AS avg_txn_amount,
        MIN(txn_amount) AS min_txn_amount,
        MAX(txn_amount) AS max_txn_amount,
        COUNT(DISTINCT card_id) AS unique_cards,
        COUNT(DISTINCT card_id) FILTER (WHERE is_fraud = TRUE) AS compromised_cards
    FROM fact_transactions
    WHERE txn_timestamp >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY merchant_category, merchant_id
),
ranked AS (
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY fraud_rate_pct) AS fraud_rate_decile,
        (fraud_rate_pct - AVG(fraud_rate_pct) OVER ()) / 
            NULLIF(STDDEV(fraud_rate_pct) OVER (), 0) AS fraud_rate_zscore
    FROM merchant_stats
)
SELECT
    *,
    -- Final risk tier assignment using multiple criteria
    CASE
        WHEN fraud_rate_decile >= 0.9 
             AND fraud_rate_zscore > 2.0 THEN 'CRITICAL_RISK'       -- Top decile AND statistical outlier
        WHEN fraud_rate_decile >= 0.8 THEN 'HIGH_RISK'              -- Top 20% fraud rate
        WHEN fraud_rate_decile >= 0.6 THEN 'ELEVATED'               -- Above median fraud rate
        ELSE 'NORMAL'
    END AS merchant_risk_tier
FROM ranked;

-- Unique index enables CONCURRENTLY refresh (no locks during refresh)
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_merchant 
    ON mv_merchant_risk_dashboard(merchant_id);

-- To refresh (run via pg_cron or application scheduler every 15 min):
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_merchant_risk_dashboard;


-- ────────────────────────────────────────────────────────────────────────────
-- 3.5 DYNAMIC PIVOT: TIME-SERIES FRAUD HEATMAP
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

CREATE OR REPLACE VIEW v_fraud_temporal_heatmap AS
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
-- SECTION 4: UTILITY QUERIES & DASHBOARDS
-- ============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- 4.1 PORTFOLIO-LEVEL FRAUD KPI SUMMARY
-- Quick executive dashboard: key fraud metrics at a glance
-- ────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW v_portfolio_fraud_kpi AS
SELECT
    COUNT(*) AS total_transactions,
    COUNT(*) FILTER (WHERE is_fraud) AS fraud_transactions,
    ROUND(AVG(CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END) * 100, 3) AS fraud_rate_pct,
    ROUND(SUM(txn_amount), 2) AS total_volume_usd,
    ROUND(SUM(txn_amount) FILTER (WHERE is_fraud), 2) AS fraud_volume_usd,
    ROUND(AVG(txn_amount), 2) AS avg_txn_amount,
    ROUND(AVG(txn_amount) FILTER (WHERE is_fraud), 2) AS avg_fraud_amount,
    COUNT(DISTINCT card_id) AS unique_cards,
    COUNT(DISTINCT card_id) FILTER (WHERE is_fraud) AS compromised_cards,
    ROUND(COUNT(DISTINCT card_id) FILTER (WHERE is_fraud)::DECIMAL / 
          COUNT(DISTINCT card_id) * 100, 2) AS card_compromise_rate_pct
FROM fact_transactions
WHERE txn_timestamp >= CURRENT_DATE - INTERVAL '30 days';


-- ────────────────────────────────────────────────────────────────────────────
-- 4.2 ROLLING 7-DAY FRAUD TREND
-- Daily fraud metrics with 7-day moving average for trend analysis
-- ────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW v_fraud_7day_trend AS
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
    ), 3) AS ma_7d_fraud_rate,
    ROUND(AVG(daily_fraud_rate) OVER (
        ORDER BY txn_date
        ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
    ), 3) AS ma_30d_fraud_rate
FROM daily
ORDER BY txn_date DESC;


-- ────────────────────────────────────────────────────────────────────────────
-- 4.3 CARD-LEVEL RISK PROFILE
-- Generate a comprehensive risk profile for any individual card
-- Useful for fraud investigation and customer service teams
-- ────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW v_card_risk_profile AS
SELECT
    c.card_id,
    c.card_hash,
    c.card_type,
    c.card_network,
    c.issuing_bank,
    c.bin_country,
    c.risk_tier,
    COUNT(t.txn_id) AS total_txns,
    COUNT(t.txn_id) FILTER (WHERE t.is_fraud) AS fraud_txns,
    ROUND(COUNT(t.txn_id) FILTER (WHERE t.is_fraud)::DECIMAL / 
          NULLIF(COUNT(t.txn_id), 0) * 100, 2) AS fraud_rate_pct,
    ROUND(SUM(t.txn_amount), 2) AS total_spend,
    ROUND(AVG(t.txn_amount), 2) AS avg_txn,
    ROUND(STDDEV(t.txn_amount), 2) AS txn_amount_stddev,
    MIN(t.txn_timestamp) AS first_txn,
    MAX(t.txn_timestamp) AS last_txn,
    COUNT(DISTINCT t.txn_country) AS countries_used,
    COUNT(DISTINCT t.merchant_category) AS merchant_categories
FROM dim_cards c
LEFT JOIN fact_transactions t ON c.card_id = t.card_id
GROUP BY c.card_id, c.card_hash, c.card_type, c.card_network, 
         c.issuing_bank, c.bin_country, c.risk_tier
ORDER BY fraud_txns DESC;


-- ============================================================================
-- SECTION 5: PYTHON DATA LOADER SCRIPT
-- Use this to load CSV data from GitHub directly into PostgreSQL
-- ============================================================================

-- Python script (copy below into a file: `load_fraud_data.py`)
-- 
-- #!/usr/bin/env python3
-- """
-- Load IEEE-CIS Fraud Detection CSV from GitHub into PostgreSQL
-- Usage: python3 load_fraud_data.py
-- """
-- 
-- import psycopg2
-- import pandas as pd
-- import io
-- import requests
-- from dotenv import load_dotenv
-- import os
-- 
-- load_dotenv()
-- 
-- # GitHub raw CSV URL
-- GITHUB_CSV_URL = "https://raw.githubusercontent.com/AD9319/fraud-detection/main/transactions.csv"
-- 
-- # Database connection parameters
-- DB_CONFIG = {
--     "host": os.getenv("DB_HOST", "localhost"),
--     "port": os.getenv("DB_PORT", "5432"),
--     "database": os.getenv("DB_NAME", "fraud_analytics"),
--     "user": os.getenv("DB_USER", "postgres"),
--     "password": os.getenv("DB_PASSWORD", ""),
-- }
-- 
-- def load_csv_from_github():
--     """Download CSV from GitHub and load into PostgreSQL"""
--     try:
--         print(f"📥 Downloading CSV from {GITHUB_CSV_URL}...")
--         response = requests.get(GITHUB_CSV_URL)
--         response.raise_for_status()
--         
--         # Read CSV into pandas
--         df = pd.read_csv(io.StringIO(response.text))
--         print(f"✅ Downloaded {len(df)} records")
--         
--         # Connect to PostgreSQL
--         conn = psycopg2.connect(**DB_CONFIG)
--         cursor = conn.cursor()
--         
--         # Clear staging table
--         cursor.execute("DELETE FROM fraud_analytics.stg_raw_transactions;")
--         
--         # Insert data using executemany for efficiency
--         insert_query = """
--             INSERT INTO fraud_analytics.stg_raw_transactions VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
--         """
--         
--         rows = [tuple(row) for row in df.values]
--         cursor.executemany(insert_query, rows)
--         conn.commit()
--         
--         print(f"✅ Inserted {cursor.rowcount} records into stg_raw_transactions")
--         
--         # Transform and populate dimension/fact tables
--         cursor.execute("""
--             INSERT INTO fraud_analytics.dim_cards (card_hash, card_type, card_network, issuing_bank, bin_country)
--             SELECT DISTINCT CardHash, CardType, CardNetwork, IssuingBank, TransactionCountry
--             FROM fraud_analytics.stg_raw_transactions
--             ON CONFLICT (card_hash) DO NOTHING;
--         """)
--         
--         cursor.execute("""
--             INSERT INTO fraud_analytics.dim_identity (device_type, device_info, ip_address, email_domain, browser, os_type)
--             SELECT DISTINCT DeviceType, DeviceHash, IPAddress::INET, EmailDomain, Browser, OSType
--             FROM fraud_analytics.stg_raw_transactions
--             ON CONFLICT (device_info, ip_address, email_domain) DO NOTHING;
--         """)
--         
--         cursor.execute("""
--             INSERT INTO fraud_analytics.fact_transactions (card_id, identity_id, txn_timestamp, txn_amount, merchant_category, is_fraud)
--             SELECT dc.card_id, di.identity_id, stg.TransactionTime, stg.TransactionAmount, stg.MerchantCategory, stg.IsFraud
--             FROM fraud_analytics.stg_raw_transactions stg
--             JOIN fraud_analytics.dim_cards dc ON stg.CardHash = dc.card_hash
--             JOIN fraud_analytics.dim_identity di ON stg.DeviceHash = di.device_info;
--         """)
--         
--         conn.commit()
--         print("✅ Data transformation complete!")
--         
--         cursor.close()
--         conn.close()
--         
--     except Exception as e:
--         print(f"❌ Error: {e}")
--         raise
-- 
-- if __name__ == "__main__":
--     load_csv_from_github()


-- ============================================================================
-- HOW TO RUN THIS PROJECT (COMPLETE SETUP GUIDE)
-- ============================================================================

-- STEP 1: CREATE DATABASE
-- createdb fraud_analytics
-- psql fraud_analytics < fraud_detection_complete.sql

-- STEP 2: LOAD DATA (Choose one option)
-- Option A: Direct file load (if CSV is on your machine)
--   COPY stg_raw_transactions FROM '/path/to/transactions.csv'
--   WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

-- Option B: From GitHub using Python script
--   pip install psycopg2 pandas requests python-dotenv
--   python3 load_fraud_data.py

-- Option C: Download first, then load
--   wget https://raw.githubusercontent.com/AD9319/fraud-detection/main/transactions.csv
--   psql fraud_analytics -c "COPY stg_raw_transactions FROM 'transactions.csv' WITH (FORMAT csv, HEADER true);"

-- STEP 3: VERIFY DATA
-- SELECT * FROM v_portfolio_fraud_kpi;
-- SELECT * FROM v_fraud_7day_trend LIMIT 30;
-- SELECT * FROM v_velocity_anomalies LIMIT 50;

-- STEP 4: RUN ADVANCED QUERIES
-- SELECT * FROM mv_merchant_risk_dashboard ORDER BY fraud_rate_pct DESC;
-- SELECT * FROM v_fraud_temporal_heatmap;
-- SELECT * FROM v_fraud_rings LIMIT 20;

-- ============================================================================
-- QUICK REFERENCE: KEY VIEWS & FUNCTIONS
-- ============================================================================
-- 
-- Executive Dashboard:
--   SELECT * FROM v_portfolio_fraud_kpi;
-- 
-- Fraud Trends:
--   SELECT * FROM v_fraud_7day_trend ORDER BY txn_date DESC;
-- 
-- Velocity Anomalies:
--   SELECT * FROM v_velocity_anomalies ORDER BY txn_timestamp DESC LIMIT 50;
-- 
-- Fraud Rings:
--   SELECT * FROM v_fraud_rings ORDER BY connected_cards_count DESC;
-- 
-- Merchant Risk:
--   SELECT * FROM mv_merchant_risk_dashboard ORDER BY fraud_rate_pct DESC LIMIT 20;
-- 
-- Temporal Heatmap:
--   SELECT * FROM v_fraud_temporal_heatmap;
-- 
-- Risk Score Calculator:
--   SELECT * FROM calculate_transaction_risk_score(1, 1, 150.00, 'Electronics', 'US');
-- 
-- Card Risk Profile:
--   SELECT * FROM v_card_risk_profile ORDER BY fraud_txns DESC LIMIT 100;
-- 
-- ============================================================================
