#!/usr/bin/env python3
"""
Split Files Data Loader for Snowflake
=====================================
This script loads split CSV files from the local directory into Snowflake tables
without any data loss, handling the semantic model structure properly.

Usage:
    python load_split_files.py
"""

import os
import sys
import pandas as pd
import glob
from typing import Dict, List
import logging

# Import from the existing snowflake.py
try:
    from snowflake import SnowflakeConfig, SnowflakeSchemaManager, SNOWFLAKE_AVAILABLE
except ImportError:
    # If import fails, define the classes here
    from dataclasses import dataclass
    from typing import Optional
    
    @dataclass
    class SnowflakeConfig:
        account: str
        user: str
        password: str
        warehouse: str
        database: str
        schema: str
        role: Optional[str] = None
    
    try:
        import snowflake.connector
        from snowflake.connector.pandas_tools import write_pandas
        SNOWFLAKE_AVAILABLE = True
    except ImportError:
        SNOWFLAKE_AVAILABLE = False
        print("❌ Snowflake connector not available. Please install: pip install snowflake-connector-python")
        sys.exit(1)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SplitFileLoader:
    """Loads split CSV files into Snowflake tables"""
    
    def __init__(self, snowflake_config: SnowflakeConfig, split_files_dir: str):
        self.config = snowflake_config
        self.split_files_dir = split_files_dir
        self.connection = None
        
        # Column mappings based on semantic_model.yaml
        self.column_mappings = {
            'ANON_VIEWS': {
                'app_id': 'APP_ID',
                'user_id': 'USER_ID',
                'company_id': 'COMPANY_ID',
                'status': 'STATUS',
                'company_status': 'STATUS',  # Added this mapping
                'view_time': 'VIEW_TIME',
                'view_date': 'VIEW_DATE',
                'total_view_time': 'TOTAL_VIEW_TIME_SEC',
                'total_amount_spent': 'TOTAL_AMOUNT_SPENT'
            },
            'ANON_USER_DAY_FACT': {
                'user_id': 'USER_ID',
                'company_id': 'COMPANY_ID',
                'view_date': 'VIEW_DATE',
                'company_status': 'STATUS',
                'status': 'STATUS',
                'total_amount_spent': 'TOTAL_AMOUNT_SPENT',
                'total_view_time': 'TOTAL_VIEW_TIME_SEC',
                'app_count': 'DISTINCT_APP_COUNT',
                'view_count': 'VIEW_COUNT'
            },
            'ANON_COMPANY_DAY_FACT': {
                'company_id': 'COMPANY_ID',
                'view_date': 'VIEW_DATE',
                'company_status': 'STATUS',
                'status': 'STATUS',
                'total_amount_spent': 'TOTAL_AMOUNT_SPENT',
                'total_view_time': 'TOTAL_VIEW_TIME_SEC',
                'app_count': 'DISTINCT_APP_COUNT',
                'view_count': 'VIEW_COUNT',
                'user_count': 'DISTINCT_USER_COUNT'
            }
        }
    
    def connect(self):
        """Connect to Snowflake"""
        if not SNOWFLAKE_AVAILABLE:
            raise ImportError("snowflake-connector-python is required")
        
        try:
            self.connection = snowflake.connector.connect(
                account=self.config.account,
                user=self.config.user,
                password=self.config.password,
                warehouse=self.config.warehouse,
                database=self.config.database,
                schema=self.config.schema,
                role=self.config.role
            )
            logger.info(f"✅ Connected to Snowflake: {self.config.account}")
            
            # Use the correct database and schema
            cursor = self.connection.cursor()
            cursor.execute(f"USE DATABASE {self.config.database}")
            cursor.execute(f"USE SCHEMA {self.config.schema}")
            cursor.close()
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to Snowflake: {e}")
            raise
    
    def find_split_files(self) -> Dict[str, List[str]]:
        """Find all split files organized by table"""
        file_groups = {
            'ANON_VIEWS': [],
            'ANON_USER_DAY_FACT': [],
            'ANON_COMPANY_DAY_FACT': []
        }
        
        if not os.path.exists(self.split_files_dir):
            logger.error(f"Split files directory not found: {self.split_files_dir}")
            return file_groups
        
        # Find all CSV files in the directory
        csv_files = glob.glob(os.path.join(self.split_files_dir, "*.csv"))
        
        for file_path in csv_files:
            filename = os.path.basename(file_path).lower()
            
            if 'anon_views' in filename:
                file_groups['ANON_VIEWS'].append(file_path)
            elif 'anon_user_day_fact' in filename or 'user_day_fact' in filename:
                file_groups['ANON_USER_DAY_FACT'].append(file_path)
            elif 'anon_company_day_fact' in filename or 'company_day_fact' in filename:
                file_groups['ANON_COMPANY_DAY_FACT'].append(file_path)
        
        # Sort files to ensure consistent loading order
        for table in file_groups:
            file_groups[table].sort()
        
        return file_groups
    
    def map_columns(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Map CSV column names to schema column names"""
        if table_name in self.column_mappings:
            mapping = self.column_mappings[table_name]
            
            # Create a case-insensitive mapping
            current_columns = {col.lower(): col for col in df.columns}
            rename_dict = {}
            
            for csv_col, schema_col in mapping.items():
                if csv_col.lower() in current_columns:
                    rename_dict[current_columns[csv_col.lower()]] = schema_col
            
            df = df.rename(columns=rename_dict)
            logger.info(f"Applied column mapping for {table_name}: {rename_dict}")
        
        # Convert all column names to uppercase (Snowflake convention)
        df.columns = [col.upper() for col in df.columns]
        
        return df
    
    def load_files_for_table(self, table_name: str, file_paths: List[str]) -> int:
        """Load all files for a specific table"""
        if not file_paths:
            logger.warning(f"No files found for table {table_name}")
            return 0
        
        if not self.connection:
            self.connect()
        
        total_rows = 0
        
        logger.info(f"Loading {len(file_paths)} files for table {table_name}")
        
        for i, file_path in enumerate(file_paths, 1):
            try:
                logger.info(f"Loading file {i}/{len(file_paths)}: {os.path.basename(file_path)}")
                
                # Read CSV file
                df = pd.read_csv(file_path)
                logger.info(f"Read {len(df)} rows from {os.path.basename(file_path)}")
                
                # Map columns
                df = self.map_columns(df, table_name)
                
                # Handle data type conversions
                df = self.convert_data_types(df, table_name)
                
                # Load to Snowflake
                success, nchunks, nrows, _ = write_pandas(
                    self.connection,
                    df,
                    table_name,
                    auto_create_table=False,
                    overwrite=False  # Append to existing table
                )
                
                if success:
                    total_rows += nrows
                    logger.info(f"✅ Loaded {nrows} rows from {os.path.basename(file_path)}")
                else:
                    logger.error(f"❌ Failed to load {os.path.basename(file_path)}")
                
            except Exception as e:
                logger.error(f"❌ Error loading {file_path}: {e}")
                continue
        
        logger.info(f"✅ Total rows loaded for {table_name}: {total_rows}")
        return total_rows
    
    def convert_data_types(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Convert data types to match Snowflake schema"""
        try:
            # Convert date columns
            if 'VIEW_DATE' in df.columns:
                df['VIEW_DATE'] = pd.to_datetime(df['VIEW_DATE']).dt.date
            
            # Convert timestamp columns
            if 'VIEW_TIME' in df.columns:
                df['VIEW_TIME'] = pd.to_datetime(df['VIEW_TIME'])
            
            # Convert numeric columns
            numeric_columns = ['TOTAL_AMOUNT_SPENT', 'TOTAL_VIEW_TIME_SEC', 'TOTAL_VIEW_TIME', 
                             'DISTINCT_APP_COUNT', 'VIEW_COUNT', 'DISTINCT_USER_COUNT', 'APP_COUNT', 'USER_COUNT']
            
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Handle string columns - ensure they're strings
            string_columns = ['APP_ID', 'USER_ID', 'COMPANY_ID', 'STATUS']
            for col in string_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str)
            
        except Exception as e:
            logger.warning(f"Data type conversion warning for {table_name}: {e}")
        
        return df
    
    def verify_data_load(self) -> Dict[str, int]:
        """Verify data was loaded correctly"""
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        row_counts = {}
        
        tables = ['ANON_VIEWS', 'ANON_USER_DAY_FACT', 'ANON_COMPANY_DAY_FACT']
        
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                row_counts[table] = count
                logger.info(f"Table {table}: {count:,} rows")
            except Exception as e:
                logger.error(f"Error counting rows in {table}: {e}")
                row_counts[table] = -1
        
        cursor.close()
        return row_counts
    
    def clear_tables(self):
        """Clear existing data from tables (optional)"""
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        tables = ['ANON_VIEWS', 'ANON_USER_DAY_FACT', 'ANON_COMPANY_DAY_FACT']
        
        for table in tables:
            try:
                cursor.execute(f"TRUNCATE TABLE {table}")
                logger.info(f"✅ Cleared table {table}")
            except Exception as e:
                logger.error(f"Error clearing table {table}: {e}")
        
        cursor.close()
    
    def load_all_data(self, clear_existing: bool = False) -> Dict[str, int]:
        """Load all split files into their respective tables"""
        logger.info("🚀 Starting split files data loading process...")
        
        # Clear existing data if requested
        if clear_existing:
            logger.info("Clearing existing data from tables...")
            self.clear_tables()
        
        # Find all split files
        file_groups = self.find_split_files()
        
        # Display found files
        for table, files in file_groups.items():
            logger.info(f"Found {len(files)} files for {table}")
            for file_path in files:
                logger.info(f"  - {os.path.basename(file_path)}")
        
        # Load data for each table
        results = {}
        for table, files in file_groups.items():
            if files:
                results[table] = self.load_files_for_table(table, files)
            else:
                results[table] = 0
        
        # Verify the load
        logger.info("Verifying data load...")
        verification = self.verify_data_load()
        
        return results
    
    def close(self):
        """Close Snowflake connection"""
        if self.connection:
            self.connection.close()
            logger.info("Closed Snowflake connection")

def main():
    """Main function"""
    print("🔄 Split Files Data Loader for Snowflake")
    print("=" * 50)
    
    # Configuration
    split_files_directory = r"c:\Users\vaddi\OneDrive\Desktop\dbeaver\anon_app_platform_dataset\split_files"
    
    snowflake_config = SnowflakeConfig(
        account="jsovftw-uu47247",
        user="manideep285",
        password="v.Manideep@123",
        warehouse="COMPUTE_WH",
        database="ANON_APP_PLATFORM",
        schema="PUBLIC",
        role="ACCOUNTADMIN"
    )
    
    # Create loader
    loader = SplitFileLoader(snowflake_config, split_files_directory)
    
    try:
        # Ask user if they want to clear existing data
        clear_data = input("Do you want to clear existing data before loading? (y/N): ").lower().strip()
        clear_existing = clear_data in ['y', 'yes']
        
        if clear_existing:
            print("⚠️  Will clear existing data and reload everything")
        else:
            print("📝 Will append to existing data")
        
        # Load all data
        results = loader.load_all_data(clear_existing=clear_existing)
        
        # Display results
        print("\n" + "=" * 60)
        print("✅ Data loading completed!")
        print("\n📊 Loading Results:")
        for table, count in results.items():
            print(f"  - {table}: {count:,} rows loaded")
        
        print("\n🎯 You can now query your data in Snowflake!")
        print("=" * 60)
        
    except Exception as e:
        logger.error(f"❌ Error during data loading: {e}")
        print(f"❌ Failed to load data: {e}")
        sys.exit(1)
    finally:
        loader.close()

if __name__ == "__main__":
    main()