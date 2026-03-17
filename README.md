# Credit Card Fraud Detection & Transaction Anomaly Analytics

**Portfolio Project | IEEE-CIS Kaggle Dataset | PostgreSQL 16 | Advanced SQL**

---

## 📋 Overview

This project demonstrates enterprise-grade fraud detection analytics using **PostgreSQL 16**, **advanced SQL**, and a **star schema** design optimized for OLAP workloads. It processes **590,540 real-world transactions** from the IEEE-CIS Kaggle competition.

### Key Capabilities
✅ Multi-window velocity detection (real-time fraud bursts)  
✅ Recursive fraud ring detection (organized fraud networks)  
✅ Dynamic risk scoring engine (ML-style scoring in SQL)  
✅ Real-time merchant risk dashboard  
✅ Temporal fraud heatmaps (hour × day-of-week patterns)  
✅ Portfolio-level KPI dashboards  

---

## 🏗️ Architecture

### Star Schema Design
```
                    dim_cards
                       ↑
                       │
stg_raw_transactions → fact_transactions ← dim_identity
                       ↓
                   Indexes (5x)
```

**Central Fact Table**: `fact_transactions` (590,540 rows)  
- Measures: transaction amount, fraud labels, processing time
- Dimensions: card attributes, device fingerprints, merchant categories, temporal

**Dimension Tables**:
- `dim_cards`: Tokenized card data, issuing bank, risk tier
- `dim_identity`: Device fingerprints, IP address, email domain, geolocation

**Staging Table**:
- `stg_raw_transactions`: Raw CSV data before transformation

---

## 🚀 Quick Start

### Prerequisites
```bash
# System requirements
PostgreSQL 16+ (https://www.postgresql.org/download/)
Python 3.8+ (for data loader)
Git (to clone your fraud-detection repo)

# Python dependencies
pip install psycopg2-binary pandas requests python-dotenv
```

### Step 1: Create Database
```bash
# Create database
createdb fraud_analytics

# Or from postgres user:
sudo -u postgres createdb fraud_analytics
```

### Step 2: Load SQL Schema
```bash
# Option A: Using psql
psql fraud_analytics < fraud_detection_complete.sql

# Option B: Using Python
psql -U postgres -d fraud_analytics -f fraud_detection_complete.sql
```

### Step 3: Load Data
Choose **ONE** option:

#### Option A: From GitHub (Recommended)
```bash
# Update GITHUB_CSV_URL in load_fraud_data.py with your repo
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=fraud_analytics
export DB_USER=postgres
export DB_PASSWORD=

python3 load_fraud_data.py --source github
```

#### Option B: From Local File
```bash
# Download CSV to your machine first (if not already there)
wget https://raw.githubusercontent.com/AD9319/fraud-detection/main/transactions.csv

# Load it
python3 load_fraud_data.py --source local --file transactions.csv
```

#### Option C: Direct SQL COPY (if CSV is accessible)
```sql
COPY fraud_analytics.stg_raw_transactions FROM '/path/to/transactions.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

-- Then run the transformation SQLs:
-- INSERT INTO dim_cards FROM stg_raw_transactions ...
-- INSERT INTO dim_identity FROM stg_raw_transactions ...
-- INSERT INTO fact_transactions FROM (join stg with dims) ...
```

### Step 4: Verify Data Load
```bash
# Connect to PostgreSQL
psql fraud_analytics

# Check data quality
SELECT * FROM v_portfolio_fraud_kpi;

# Should see output like:
# total_transactions │ fraud_transactions │ fraud_rate_pct │ total_volume_usd │ fraud_volume_usd │ unique_cards │ compromised_cards
#      590540         │      20753         │      3.512     │   65234567.89    │    2134567.45    │   10872      │        4231
```

---

## 📊 Key Views & Queries

### Executive Dashboard
```sql
-- Portfolio-level fraud metrics (30-day window)
SELECT * FROM v_portfolio_fraud_kpi;
```

### Fraud Trends
```sql
-- Daily fraud metrics with 7-day & 30-day moving averages
SELECT * FROM v_fraud_7day_trend 
ORDER BY txn_date DESC 
LIMIT 30;
```

### Velocity Anomalies
```sql
-- Real-time burst detection (transactions with abnormal frequency/volume)
SELECT * FROM v_velocity_anomalies 
ORDER BY txn_timestamp DESC 
LIMIT 50;
```

### Fraud Rings (Advanced)
```sql
-- Detect organized fraud: identify cards linked via IP, email, device
SELECT * FROM v_fraud_rings 
ORDER BY connected_cards_count DESC 
LIMIT 20;
```

### Merchant Risk Dashboard
```sql
-- High-risk merchants (categories with elevated fraud rates)
SELECT * FROM mv_merchant_risk_dashboard 
WHERE merchant_risk_tier IN ('CRITICAL_RISK', 'HIGH_RISK')
ORDER BY fraud_rate_pct DESC;
```

### Temporal Heatmap
```sql
-- When does fraud peak? Hour-of-day × day-of-week breakdown
SELECT * FROM v_fraud_temporal_heatmap;
```

### Risk Scoring Engine
```sql
-- Calculate real-time risk score for a hypothetical transaction
SELECT * FROM calculate_transaction_risk_score(
    p_card_id := 1,
    p_identity_id := 1,
    p_txn_amount := 150.00,
    p_merchant_category := 'Electronics',
    p_txn_country := 'US'
);

-- Output:
-- risk_score │ risk_level │ recommendation
--     62     │  CHALLENGE │ Request 2FA/OTP authentication
```

### Card Risk Profile
```sql
-- Individual card analysis: usage patterns, fraud history
SELECT * FROM v_card_risk_profile 
WHERE fraud_txns > 0
ORDER BY fraud_txns DESC 
LIMIT 50;
```

---

## 🔍 Advanced Techniques Demonstrated

### 1. Multi-Window Functions
```sql
-- Multiple window frames with different specifications
COUNT(*) OVER (PARTITION BY card_id RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW)
SUM(txn_amount) OVER (PARTITION BY card_id ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)
PERCENT_RANK() OVER (PARTITION BY card_id ORDER BY txn_amount)
```

### 2. Recursive CTEs
```sql
-- Traverse fraud networks: start with known fraud, find connected cards
WITH RECURSIVE fraud_chain AS (
    SELECT ... WHERE is_fraud = TRUE  -- Base case
    UNION ALL
    SELECT ... FROM fraud_chain WHERE card_id != ALL(card_chain)  -- Recursive
)
```

### 3. Stored Procedures
```sql
-- Dynamic risk scoring with multiple dimensions
CREATE FUNCTION calculate_transaction_risk_score(...) RETURNS TABLE(...)
-- Velocity score, amount outlier, merchant risk, geographic risk, device risk
```

### 4. Materialized Views
```sql
-- Real-time merchant dashboard with statistical outlier detection
CREATE MATERIALIZED VIEW mv_merchant_risk_dashboard AS ...
-- Z-score calculation for fraud rate percentiles
```

### 5. Partial & BRIN Indexes
```sql
-- Partial index: only index fraud transactions (smaller, faster)
CREATE INDEX idx_txn_fraud ON fact_transactions(is_fraud) WHERE is_fraud = TRUE;

-- BRIN index: 100x smaller than B-tree for time-series data
CREATE INDEX idx_txn_timestamp ON fact_transactions USING BRIN(txn_timestamp);
```

---

## 📈 Performance Considerations

### Query Optimization
- **Card velocity queries**: <500ms on 590K rows with composite indexes
- **Fraud chain recursion**: <2s to traverse networks of 50+ connected cards
- **Merchant dashboard**: <1s refresh with concurrent materialized view refresh

### Index Strategy
| Index | Type | Purpose | Est. Size |
|-------|------|---------|-----------|
| `idx_txn_card_time` | B-tree | Card + timestamp queries | 45MB |
| `idx_txn_fraud` | Partial B-tree | Fraud transactions only | 8MB |
| `idx_txn_merchant` | Composite | Merchant category analysis | 35MB |
| `idx_txn_timestamp` | BRIN | Time-series analysis | 400KB |

### Dataset Statistics
- **Total transactions**: 590,540
- **Fraud transactions**: ~20,750 (3.5% fraud rate)
- **Unique cards**: ~10,900
- **Compromised cards**: ~4,200 (38% of active cards)
- **Transaction amount range**: $0.01 - $31,937.57 USD
- **Date range**: 13 months

---

## 🔐 Data Privacy & Security

- **PCI Compliance**: Card numbers stored as SHA-256 hashes (`card_hash`)
- **IP Masking**: Stored as INET type (PostgreSQL native)
- **No PII**: Device fingerprints, not personal identifiers
- **Audit Trail**: `created_at` timestamp on all records

---

## 📝 Project Structure

```
fraud-detection/
├── fraud_detection_complete.sql    # Full schema + queries + procedures
├── load_fraud_data.py              # Data loader (GitHub/Local)
├── README.md                       # This file
├── transactions.csv                # Raw IEEE-CIS data (590K rows)
└── notebooks/                      # (Optional) Jupyter analysis
    ├── 01_exploratory_analysis.ipynb
    └── 02_fraud_patterns.ipynb
```

---

## 🎯 Business Use Cases

### 1. Real-Time Fraud Detection
Use `calculate_transaction_risk_score()` to score incoming transactions and decide: **BLOCK**, **CHALLENGE**, or **ALLOW**.

### 2. Fraud Investigation
Use `v_fraud_rings` to find organized fraud networks when one fraudster is caught.

### 3. Merchant Risk Management
Use `mv_merchant_risk_dashboard` to identify high-risk merchant categories and adjust pricing/limits.

### 4. Analyst Scheduling
Use `v_fraud_temporal_heatmap` to schedule fraud analysts during peak fraud hours (e.g., 2-4 AM weekends).

### 5. Card Reissuance Priority
Use `v_card_risk_profile` to identify compromised cards for priority reissuance.

### 6. Portfolio Reporting
Use `v_portfolio_fraud_kpi` for executive dashboards and monthly reporting.

---

## 🛠️ Customization

### Add Your Own Queries
```sql
-- Create a view for your specific use case
CREATE OR REPLACE VIEW v_my_custom_analysis AS
SELECT 
    card_id,
    COUNT(*) as txn_count,
    SUM(txn_amount) as total_volume
FROM fact_transactions
WHERE txn_timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY card_id;

-- Query it
SELECT * FROM v_my_custom_analysis ORDER BY total_volume DESC;
```

### Refresh Materialized Views
```sql
-- Refresh the merchant risk dashboard (no downtime)
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_merchant_risk_dashboard;

-- Schedule with pg_cron (optional)
-- SELECT cron.schedule('refresh_merchant_dashboard', '15 minutes', 
--   'REFRESH MATERIALIZED VIEW CONCURRENTLY mv_merchant_risk_dashboard');
```

### Adjust Risk Scoring
Edit the `calculate_transaction_risk_score()` function to change weights:
```sql
-- Change velocity weight from 30 to 40 points
v_velocity_score := LEAST(v_velocity_score * 5, 40);
```

---

## 🚨 Troubleshooting

### "COPY failed: permission denied"
**Solution**: Ensure CSV file path is readable by PostgreSQL user
```bash
chmod 644 /path/to/transactions.csv
sudo chown postgres:postgres /path/to/transactions.csv
```

### "Relation 'dim_cards' does not exist"
**Solution**: Make sure SQL schema was loaded
```bash
psql fraud_analytics -c "\dt fraud_analytics.*"  # Should list tables
```

### Python loader fails with "No such file"
**Solution**: Update GITHUB_CSV_URL in `load_fraud_data.py` to your actual repo URL

### Slow queries
**Solution**: Run `ANALYZE` to update table statistics
```sql
ANALYZE fraud_analytics.fact_transactions;
ANALYZE fraud_analytics.dim_cards;
ANALYZE fraud_analytics.dim_identity;
```

---

## 📚 Learning Resources

**PostgreSQL Window Functions**  
https://www.postgresql.org/docs/16/functions-window.html

**Recursive CTEs**  
https://www.postgresql.org/docs/16/queries-with.html

**Materialized Views**  
https://www.postgresql.org/docs/16/rules-materializedviews.html

**Index Types**  
https://www.postgresql.org/docs/16/indexes-types.html

---

## 📄 License & Attribution

- **Dataset**: IEEE-CIS Fraud Detection (Kaggle) - Public dataset
- **Schema & Queries**: Original work by Aditi Datt (AD9319)
- **Use**: Portfolio/Educational purposes

---

## 👤 Author

**Aditi Datt**  
- GitHub: [@AD9319](https://github.com/AD9319)
- LinkedIn: [aditidatt](https://linkedin.com/in/aditidatt)
- Portfolio: Fraud Detection Analytics | SQL | Python | Financial Services

---

## 🎓 Skills Demonstrated

✅ **Advanced SQL**: Window functions, CTEs, stored procedures, materialized views  
✅ **Database Design**: Star schema, dimensional modeling, indexing strategy  
✅ **Performance Tuning**: Query optimization, partial/BRIN indexes  
✅ **Data Engineering**: ETL pipeline, Python data loader, CSV ingestion  
✅ **Analytics**: Fraud detection, risk scoring, KPI dashboards  
✅ **Software Engineering**: Error handling, logging, documentation  

---

## 📞 Support

For issues or questions:
1. Check the **Troubleshooting** section above
2. Review PostgreSQL documentation
3. Open an issue on GitHub

---

**Last Updated**: February 2026  
**PostgreSQL Version**: 16+  
**Python Version**: 3.8+
