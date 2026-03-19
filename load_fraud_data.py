import psycopg2
import pandas as pd
import io
import requests
import argparse
import logging
from pathlib import Path
from typing import Optional
import os
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


GITHUB_CSV_URL = os.getenv(
    "GITHUB_CSV_URL",
    "https://raw.githubusercontent.com/AD9319/fraud-detection/main/transactions.csv"
)

# Database connection parameters
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "fraud_analytics"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}


class FraudDataLoader:
    """Load fraud detection data from various sources into PostgreSQL"""
    
    def __init__(self, db_config: dict):
        """Initialize database connection"""
        self.db_config = db_config
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            logger.info(f" Connected to PostgreSQL: {self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}")
        except psycopg2.Error as e:
            logger.error(f" Database connection failed: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info(" Database connection closed")
    
    def load_csv_from_github(self) -> pd.DataFrame:
        """
        Download CSV from GitHub
        
        Returns:
            DataFrame with transaction data
        """
        try:
            logger.info(f" Downloading CSV from GitHub...")
            logger.info(f"   URL: {GITHUB_CSV_URL}")
            
            response = requests.get(GITHUB_CSV_URL, timeout=30)
            response.raise_for_status()
            
            df = pd.read_csv(io.StringIO(response.text))
            logger.info(f" Downloaded {len(df):,} records from GitHub")
            
            return df
            
        except requests.RequestException as e:
            logger.error(f" Failed to download from GitHub: {e}")
            raise
        except Exception as e:
            logger.error(f" Error parsing CSV: {e}")
            raise
    
    def load_csv_from_file(self, file_path: str) -> pd.DataFrame:
        """
        Load CSV from local file
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            DataFrame with transaction data
        """
        try:
            path = Path(file_path)
            if not path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            logger.info(f" Loading CSV from local file: {file_path}")
            
            df = pd.read_csv(file_path)
            logger.info(f" Loaded {len(df):,} records from {file_path}")
            
            return df
            
        except Exception as e:
            logger.error(f" Error loading local file: {e}")
            raise
    
    def validate_dataframe(self, df: pd.DataFrame):
        """
        Validate DataFrame structure and data types
        
        Args:
            df: DataFrame to validate
        """
        required_columns = [
            'TransactionID', 'TransactionTime', 'TransactionAmount',
            'CardHash', 'CardType', 'DeviceHash', 'IPAddress',
            'EmailDomain', 'MerchantCategory', 'IsFraud'
        ]
        
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        logger.info(f" DataFrame validation passed")
        logger.info(f"   Columns: {len(df.columns)}")
        logger.info(f"   Rows: {len(df):,}")
        logger.info(f"   Memory: {df.memory_usage(deep=True).sum() / 1e6:.2f} MB")
    
    def insert_to_staging(self, df: pd.DataFrame):
        """
        Insert data into staging table
        
        Args:
            df: DataFrame to insert
        """
        try:
            logger.info(f" Inserting {len(df):,} records into staging table...")
            
            # Clear existing staging data
            self.cursor.execute("DELETE FROM fraud_analytics.stg_raw_transactions;")
            logger.info("   Cleared existing staging data")
            
            # Insert new data in batches
            batch_size = 1000
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i+batch_size]
                
                # Prepare column names
                columns = ', '.join(batch.columns)
                placeholders = ', '.join(['%s'] * len(batch.columns))
                
                insert_query = f"""
                    INSERT INTO fraud_analytics.stg_raw_transactions ({columns})
                    VALUES ({placeholders})
                """
                
                rows = [tuple(row) for row in batch.values]
                self.cursor.executemany(insert_query, rows)
                
                if (i + batch_size) % (batch_size * 10) == 0:
                    logger.info(f"   Processed {i + batch_size:,} records...")
            
            self.conn.commit()
            logger.info(f" Inserted {self.cursor.rowcount:,} records into staging table")
            
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f" Insert to staging failed: {e}")
            raise
    
    def transform_dimensions(self):
        """
        Transform staging data into dimension tables
        """
        try:
            logger.info(" Transforming data into dimension tables...")
            
            # Populate dim_cards
            logger.info("   Populating dim_cards...")
            self.cursor.execute("""
                INSERT INTO fraud_analytics.dim_cards 
                    (card_hash, card_type, card_network, issuing_bank, bin_country, risk_tier)
                SELECT DISTINCT
                    "CardHash",
                    COALESCE(LOWER("CardType"), 'unknown'),
                    "CardNetwork",
                    "IssuingBank",
                    "TransactionCountry",
                    1
                FROM fraud_analytics.stg_raw_transactions
                ON CONFLICT (card_hash) DO NOTHING;
            """)
            logger.info(f"    Inserted {self.cursor.rowcount:,} unique cards")
            
            # Populate dim_identity
            logger.info("   Populating dim_identity...")
            self.cursor.execute("""
                INSERT INTO fraud_analytics.dim_identity 
                    (device_type, device_info, ip_address, email_domain, browser, os_type, 
                     screen_res, geo_latitude, geo_longitude, is_proxy)
                SELECT DISTINCT
                    "DeviceType",
                    "DeviceHash",
                    "IPAddress"::INET,
                    "EmailDomain",
                    "Browser",
                    "OSType",
                    "ScreenResolution",
                    "Latitude",
                    "Longitude",
                    COALESCE("IsProxy", FALSE)
                FROM fraud_analytics.stg_raw_transactions
                ON CONFLICT (device_info, ip_address, email_domain) DO NOTHING;
            """)
            logger.info(f"    Inserted {self.cursor.rowcount:,} unique device fingerprints")
            
            self.conn.commit()
            logger.info(" Dimension tables populated")
            
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f" Dimension transformation failed: {e}")
            raise
    
    def populate_facts(self):
        """
        Populate fact table by joining with dimensions
        """
        try:
            logger.info(" Populating fact_transactions table...")
            
            self.cursor.execute("""
                INSERT INTO fraud_analytics.fact_transactions 
                    (card_id, identity_id, txn_timestamp, txn_amount, product_category, 
                     merchant_id, merchant_category, txn_country, is_fraud, fraud_probability, processing_time_ms)
                SELECT
                    dc.card_id,
                    di.identity_id,
                    stg."TransactionTime",
                    stg."TransactionAmount",
                    stg."ProductCategory",
                    stg."MerchantID",
                    stg."MerchantCategory",
                    stg."TransactionCountry",
                    stg."IsFraud",
                    stg."FraudProbability",
                    stg."ProcessingTime"
                FROM fraud_analytics.stg_raw_transactions stg
                INNER JOIN fraud_analytics.dim_cards dc ON stg."CardHash" = dc.card_hash
                INNER JOIN fraud_analytics.dim_identity di ON stg."DeviceHash" = di.device_info 
                                                              AND stg."IPAddress"::INET = di.ip_address
                                                              AND stg."EmailDomain" = di.email_domain
                ON CONFLICT DO NOTHING;
            """)
            
            self.conn.commit()
            logger.info(f" Inserted {self.cursor.rowcount:,} transactions into fact table")
            
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f" Fact table population failed: {e}")
            raise
    
    def validate_load(self):
        """
        Validate data quality and completeness
        """
        try:
            logger.info(" Running data quality validation...")
            
            self.cursor.execute("""
                SELECT 
                    COUNT(*) as total_transactions,
                    COUNT(*) FILTER (WHERE is_fraud = TRUE) as fraud_transactions,
                    ROUND(AVG(CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END) * 100, 3) as fraud_rate_pct,
                    COUNT(DISTINCT card_id) as unique_cards,
                    COUNT(DISTINCT card_id) FILTER (WHERE is_fraud = TRUE) as compromised_cards
                FROM fraud_analytics.fact_transactions;
            """)
            
            result = self.cursor.fetchone()
            
            logger.info(" Data Quality Report:")
            logger.info(f"   Total Transactions: {result[0]:,}")
            logger.info(f"   Fraud Transactions: {result[1]:,}")
            logger.info(f"   Fraud Rate: {result[2]:.3f}%")
            logger.info(f"   Unique Cards: {result[3]:,}")
            logger.info(f"   Compromised Cards: {result[4]:,}")
            
        except psycopg2.Error as e:
            logger.error(f" Validation failed: {e}")
            raise
    
    def load_from_github(self):
        """Complete pipeline: GitHub → Staging → Dimensions → Facts"""
        try:
            self.connect()
            
            # Download and validate
            df = self.load_csv_from_github()
            self.validate_dataframe(df)
            
            # Load pipeline
            self.insert_to_staging(df)
            self.transform_dimensions()
            self.populate_facts()
            
            # Validate results
            self.validate_load()
            
            logger.info(" Data load completed successfully!")
            
        except Exception as e:
            logger.error(f" Load pipeline failed: {e}")
            raise
        finally:
            self.disconnect()
    
    def load_from_file(self, file_path: str):
        """Complete pipeline: Local File → Staging → Dimensions → Facts"""
        try:
            self.connect()
            
            # Load and validate
            df = self.load_csv_from_file(file_path)
            self.validate_dataframe(df)
            
            # Load pipeline
            self.insert_to_staging(df)
            self.transform_dimensions()
            self.populate_facts()
            
            # Validate results
            self.validate_load()
            
            logger.info("🎉 Data load completed successfully!")
            
        except Exception as e:
            logger.error(f" Load pipeline failed: {e}")
            raise
        finally:
            self.disconnect()


def main():
    """Command-line interface"""
    parser = argparse.ArgumentParser(
        description="Load IEEE-CIS Fraud Detection CSV into PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load from GitHub
  python3 load_fraud_data.py --source github
  
  # Load from local file
  python3 load_fraud_data.py --source local --file transactions.csv
  
  # Override database host
  python3 load_fraud_data.py --source github --db-host myserver.com
        """
    )
    
    parser.add_argument(
        '--source',
        choices=['github', 'local'],
        default='github',
        help='Data source: github or local file'
    )
    parser.add_argument(
        '--file',
        type=str,
        help='Path to local CSV file (required if --source local)'
    )
    parser.add_argument(
        '--db-host',
        type=str,
        default='localhost',
        help='PostgreSQL host'
    )
    parser.add_argument(
        '--db-port',
        type=int,
        default=5432,
        help='PostgreSQL port'
    )
    parser.add_argument(
        '--db-name',
        type=str,
        default='fraud_analytics',
        help='Database name'
    )
    parser.add_argument(
        '--db-user',
        type=str,
        default='postgres',
        help='PostgreSQL user'
    )
    
    args = parser.parse_args()
    
    # Update DB config from command line args
    db_config = {
        "host": args.db_host,
        "port": args.db_port,
        "database": args.db_name,
        "user": args.db_user,
        "password": os.getenv("DB_PASSWORD", ""),
    }
    
    # Run loader
    loader = FraudDataLoader(db_config)
    
    if args.source == 'github':
        loader.load_from_github()
    elif args.source == 'local':
        if not args.file:
            parser.error("--file is required when --source is 'local'")
        loader.load_from_file(args.file)


if __name__ == "__main__":
    main()
