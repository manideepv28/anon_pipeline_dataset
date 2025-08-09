#!/usr/bin/env python3
"""
Data Integrity Verification Script
==================================
This script verifies that all data was loaded correctly from split files
without any loss or duplication.
"""

import os
import pandas as pd
import glob
from typing import Dict, List
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    import snowflake.connector
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    print("❌ Snowflake connector not available")

class DataIntegrityChecker:
    """Checks data integrity between local files and Snowflake tables"""
    
    def __init__(self, split_files_dir: str, snowflake_config):
        self.split_files_dir = split_files_dir
        self.config = snowflake_config
        self.connection = None
    
    def connect_snowflake(self):
        """Connect to Snowflake"""
        if not SNOWFLAKE_AVAILABLE:
            raise ImportError("snowflake-connector-python is required")
        
        self.connection = snowflake.connector.connect(
            account=self.config.account,
            user=self.config.user,
            password=self.config.password,
            warehouse=self.config.warehouse,
            database=self.config.database,
            schema=self.config.schema,
            role=self.config.role
        )
        
        cursor = self.connection.cursor()
        cursor.execute(f"USE DATABASE {self.config.database}")
        cursor.execute(f"USE SCHEMA {self.config.schema}")
        cursor.close()
    
    def count_local_files(self) -> Dict[str, int]:
        """Count rows in all local split files"""
        file_groups = {
            'ANON_VIEWS': [],
            'ANON_USER_DAY_FACT': [],
            'ANON_COMPANY_DAY_FACT': []
        }
        
        # Find files
        csv_files = glob.glob(os.path.join(self.split_files_dir, "*.csv"))
        
        for file_path in csv_files:
            filename = os.path.basename(file_path).lower()
            
            if 'anon_views' in filename:
                file_groups['ANON_VIEWS'].append(file_path)
            elif 'user_day_fact' in filename:
                file_groups['ANON_USER_DAY_FACT'].append(file_path)
            elif 'company_day_fact' in filename:
                file_groups['ANON_COMPANY_DAY_FACT'].append(file_path)
        
        # Count rows
        local_counts = {}
        for table, files in file_groups.items():
            total_rows = 0
            for file_path in files:
                try:
                    df = pd.read_csv(file_path)
                    total_rows += len(df)
                    logger.info(f"Local file {os.path.basename(file_path)}: {len(df)} rows")
                except Exception as e:
                    logger.error(f"Error reading {file_path}: {e}")
            
            local_counts[table] = total_rows
            logger.info(f"Total local rows for {table}: {total_rows}")
        
        return local_counts
    
    def count_snowflake_tables(self) -> Dict[str, int]:
        """Count rows in Snowflake tables"""
        if not self.connection:
            self.connect_snowflake()
        
        cursor = self.connection.cursor()
        snowflake_counts = {}
        
        tables = ['ANON_VIEWS', 'ANON_USER_DAY_FACT', 'ANON_COMPANY_DAY_FACT']
        
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                snowflake_counts[table] = count
                logger.info(f"Snowflake table {table}: {count} rows")
            except Exception as e:
                logger.error(f"Error counting {table}: {e}")
                snowflake_counts[table] = -1
        
        cursor.close()
        return snowflake_counts
    
    def check_for_duplicates(self) -> Dict[str, int]:
        """Check for duplicate records in Snowflake tables"""
        if not self.connection:
            self.connect_snowflake()
        
        cursor = self.connection.cursor()
        duplicate_counts = {}
        
        # Define unique key columns for each table
        unique_keys = {
            'ANON_VIEWS': ['APP_ID', 'USER_ID', 'VIEW_TIME'],
            'ANON_USER_DAY_FACT': ['USER_ID', 'VIEW_DATE'],
            'ANON_COMPANY_DAY_FACT': ['COMPANY_ID', 'VIEW_DATE']
        }
        
        for table, key_cols in unique_keys.items():
            try:
                key_list = ', '.join(key_cols)
                query = f"""
                SELECT COUNT(*) - COUNT(DISTINCT {key_list}) as duplicates 
                FROM {table}
                """
                cursor.execute(query)
                duplicates = cursor.fetchone()[0]
                duplicate_counts[table] = duplicates
                
                if duplicates > 0:
                    logger.warning(f"⚠️  Found {duplicates} duplicate records in {table}")
                else:
                    logger.info(f"✅ No duplicates found in {table}")
                    
            except Exception as e:
                logger.error(f"Error checking duplicates in {table}: {e}")
                duplicate_counts[table] = -1
        
        cursor.close()
        return duplicate_counts
    
    def sample_data_check(self) -> Dict[str, Dict]:
        """Sample data from each table for manual verification"""
        if not self.connection:
            self.connect_snowflake()
        
        cursor = self.connection.cursor()
        samples = {}
        
        tables = ['ANON_VIEWS', 'ANON_USER_DAY_FACT', 'ANON_COMPANY_DAY_FACT']
        
        for table in tables:
            try:
                cursor.execute(f"SELECT * FROM {table} LIMIT 5")
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                samples[table] = {
                    'columns': columns,
                    'sample_rows': rows
                }
                
                logger.info(f"Sample data from {table}:")
                logger.info(f"  Columns: {', '.join(columns)}")
                logger.info(f"  Sample rows: {len(rows)}")
                
            except Exception as e:
                logger.error(f"Error sampling {table}: {e}")
                samples[table] = {'error': str(e)}
        
        cursor.close()
        return samples
    
    def run_full_verification(self) -> Dict:
        """Run complete data integrity verification"""
        logger.info("🔍 Starting data integrity verification...")
        
        results = {
            'local_counts': {},
            'snowflake_counts': {},
            'duplicate_counts': {},
            'integrity_status': {},
            'samples': {}
        }
        
        try:
            # Count local files
            logger.info("Counting local file rows...")
            results['local_counts'] = self.count_local_files()
            
            # Count Snowflake tables
            logger.info("Counting Snowflake table rows...")
            results['snowflake_counts'] = self.count_snowflake_tables()
            
            # Check for duplicates
            logger.info("Checking for duplicates...")
            results['duplicate_counts'] = self.check_for_duplicates()
            
            # Compare counts
            for table in results['local_counts']:
                local_count = results['local_counts'][table]
                snowflake_count = results['snowflake_counts'].get(table, -1)
                duplicates = results['duplicate_counts'].get(table, -1)
                
                if local_count == snowflake_count and duplicates == 0:
                    results['integrity_status'][table] = 'PERFECT'
                elif local_count == snowflake_count and duplicates > 0:
                    results['integrity_status'][table] = 'DUPLICATES_FOUND'
                elif local_count != snowflake_count:
                    results['integrity_status'][table] = 'COUNT_MISMATCH'
                else:
                    results['integrity_status'][table] = 'UNKNOWN'
            
            # Get sample data
            logger.info("Sampling data for verification...")
            results['samples'] = self.sample_data_check()
            
        except Exception as e:
            logger.error(f"Error during verification: {e}")
            results['error'] = str(e)
        
        return results
    
    def close(self):
        """Close connection"""
        if self.connection:
            self.connection.close()

def main():
    """Main verification function"""
    print("🔍 Data Integrity Verification")
    print("=" * 50)
    
    # Configuration
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
    
    # Run verification
    checker = DataIntegrityChecker(split_files_directory, snowflake_config)
    
    try:
        results = checker.run_full_verification()
        
        # Display results
        print("\n" + "=" * 60)
        print("📊 VERIFICATION RESULTS")
        print("=" * 60)
        
        print("\n📁 Local Files vs Snowflake Tables:")
        for table in results['local_counts']:
            local = results['local_counts'][table]
            snowflake = results['snowflake_counts'].get(table, -1)
            status = results['integrity_status'].get(table, 'UNKNOWN')
            duplicates = results['duplicate_counts'].get(table, -1)
            
            print(f"\n{table}:")
            print(f"  Local files:     {local:,} rows")
            print(f"  Snowflake table: {snowflake:,} rows")
            print(f"  Duplicates:      {duplicates}")
            print(f"  Status:          {status}")
            
            if status == 'PERFECT':
                print("  ✅ Data integrity verified!")
            elif status == 'DUPLICATES_FOUND':
                print("  ⚠️  Duplicates found - consider deduplication")
            elif status == 'COUNT_MISMATCH':
                print("  ❌ Row count mismatch - data may be incomplete")
        
        print("\n🎯 Overall Status:")
        perfect_tables = sum(1 for status in results['integrity_status'].values() if status == 'PERFECT')
        total_tables = len(results['integrity_status'])
        
        if perfect_tables == total_tables:
            print("✅ All tables have perfect data integrity!")
        else:
            print(f"⚠️  {perfect_tables}/{total_tables} tables have perfect integrity")
        
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Verification failed: {e}")
    finally:
        checker.close()

if __name__ == "__main__":
    main()