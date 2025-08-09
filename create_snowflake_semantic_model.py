#!/usr/bin/env python3
"""
Snowflake Semantic Model Creator
================================
This script executes the SQL commands needed to prepare for semantic model creation
and provides step-by-step guidance for the Snowsight UI process.
"""

import os
import sys
import logging
from dataclasses import dataclass
from typing import Optional

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    import snowflake.connector
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    print("❌ Snowflake connector not available")
    sys.exit(1)

@dataclass
class SnowflakeConfig:
    account: str
    user: str
    password: str
    warehouse: str
    database: str
    schema: str
    role: Optional[str] = None

class SemanticModelCreator:
    """Creates semantic model infrastructure in Snowflake"""
    
    def __init__(self, snowflake_config: SnowflakeConfig):
        self.config = snowflake_config
        self.connection = None
        
    def connect(self):
        """Connect to Snowflake"""
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
        logger.info(f"✅ Connected to Snowflake: {self.config.account}")
    
    def execute_preparation_sql(self):
        """Execute SQL commands to prepare for semantic model creation"""
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        
        sql_commands = [
            # Step 1: Use database and schema
            f"USE DATABASE {self.config.database}",
            f"USE SCHEMA {self.config.schema}",
            
            # Step 2: Verify data is ready
            """
            SELECT 'ANON_VIEWS' as table_name, COUNT(*) as row_count FROM ANON_VIEWS
            UNION ALL
            SELECT 'ANON_USER_DAY_FACT', COUNT(*) FROM ANON_USER_DAY_FACT
            UNION ALL
            SELECT 'ANON_COMPANY_DAY_FACT', COUNT(*) FROM ANON_COMPANY_DAY_FACT
            """,
            
            # Step 3: Create semantic views (already done, but let's verify)
            """
            CREATE OR REPLACE VIEW SEMANTIC_ANON_VIEWS AS
            SELECT 
                APP_ID,
                USER_ID, 
                COMPANY_ID,
                STATUS,
                VIEW_TIME,
                VIEW_DATE,
                TOTAL_VIEW_TIME_SEC,
                TOTAL_AMOUNT_SPENT,
                1 as VIEW_COUNT
            FROM ANON_VIEWS
            """,
            
            """
            CREATE OR REPLACE VIEW SEMANTIC_USER_METRICS AS
            SELECT
                USER_ID,
                COMPANY_ID,
                STATUS,
                VIEW_DATE,
                TOTAL_AMOUNT_SPENT,
                TOTAL_VIEW_TIME_SEC,
                DISTINCT_APP_COUNT,
                VIEW_COUNT
            FROM ANON_USER_DAY_FACT
            """,
            
            """
            CREATE OR REPLACE VIEW SEMANTIC_COMPANY_METRICS AS
            SELECT
                COMPANY_ID,
                STATUS,
                VIEW_DATE,
                TOTAL_AMOUNT_SPENT,
                TOTAL_VIEW_TIME_SEC,
                DISTINCT_APP_COUNT,
                VIEW_COUNT,
                DISTINCT_USER_COUNT
            FROM ANON_COMPANY_DAY_FACT
            """
        ]
        
        try:
            print("🔧 Executing preparation SQL commands...")
            
            for i, sql in enumerate(sql_commands):
                if sql.strip().startswith('SELECT'):
                    # This is a verification query
                    cursor.execute(sql)
                    results = cursor.fetchall()
                    print(f"\n📊 Data Verification Results:")
                    for row in results:
                        print(f"  - {row[0]}: {row[1]:,} rows")
                elif sql.strip().startswith('CREATE OR REPLACE VIEW'):
                    # This is a view creation
                    cursor.execute(sql)
                    view_name = sql.split('VIEW ')[1].split(' AS')[0].strip()
                    print(f"✅ Created/Updated view: {view_name}")
                else:
                    # Other commands
                    cursor.execute(sql)
            
            print("\n✅ All preparation SQL commands executed successfully!")
            
        except Exception as e:
            logger.error(f"❌ Error executing SQL: {e}")
            raise
        finally:
            cursor.close()
    
    def verify_semantic_model_readiness(self):
        """Verify that everything is ready for semantic model creation"""
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        
        print("\n🔍 SEMANTIC MODEL READINESS CHECK:")
        print("="*60)
        
        try:
            # Check base tables
            base_tables = ['ANON_VIEWS', 'ANON_USER_DAY_FACT', 'ANON_COMPANY_DAY_FACT']
            print("\n📋 Base Tables:")
            for table in base_tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"  ✅ {table}: {count:,} rows")
            
            # Check semantic views
            semantic_views = ['SEMANTIC_ANON_VIEWS', 'SEMANTIC_USER_METRICS', 'SEMANTIC_COMPANY_METRICS']
            print("\n🔍 Semantic Views:")
            for view in semantic_views:
                cursor.execute(f"SELECT COUNT(*) FROM {view}")
                count = cursor.fetchone()[0]
                print(f"  ✅ {view}: {count:,} rows")
            
            # Sample data from each view
            print("\n📊 Sample Data Preview:")
            for view in semantic_views:
                cursor.execute(f"SELECT * FROM {view} LIMIT 3")
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                print(f"\n  {view} (Columns: {', '.join(columns[:5])}{'...' if len(columns) > 5 else ''}):")
                for row in results[:2]:  # Show first 2 rows
                    print(f"    Sample: {str(row)[:100]}{'...' if len(str(row)) > 100 else ''}")
            
            print("\n✅ All components are ready for semantic model creation!")
            
        except Exception as e:
            logger.error(f"❌ Error during verification: {e}")
        finally:
            cursor.close()
    
    def generate_snowsight_instructions(self):
        """Generate detailed instructions for Snowsight UI"""
        
        print("\n" + "="*80)
        print("🎯 SNOWSIGHT SEMANTIC MODEL CREATION GUIDE")
        print("="*80)
        
        instructions = [
            "\n🌐 STEP 1: Access Snowsight",
            f"   1. Open your browser and go to: https://{self.config.account}.snowflakecomputing.com",
            f"   2. Login with username: {self.config.user}",
            "   3. Make sure you're in the correct role: ACCOUNTADMIN",
            
            "\n📊 STEP 2: Navigate to Semantic Models",
            "   1. In the left sidebar, click on 'Data'",
            "   2. Look for 'Semantic Models' section",
            "   3. Click 'Create Semantic Model' or '+' button",
            
            "\n🏗️ STEP 3: Create Your Semantic Model",
            "   1. Model Name: 'PAID_APP_PLATFORM_ANON'",
            "   2. Description: 'Usage data for a paid-app platform where developers publish apps and customers view them'",
            f"   3. Database: {self.config.database}",
            f"   4. Schema: {self.config.schema}",
            
            "\n📋 STEP 4: Add Base Tables",
            "   Add these three tables as your base tables:",
            "   ",
            "   Table 1: SEMANTIC_ANON_VIEWS",
            "   - Purpose: Individual app view events",
            "   - Key Dimensions: APP_ID, USER_ID, COMPANY_ID, STATUS",
            "   - Time Dimensions: VIEW_TIME, VIEW_DATE", 
            "   - Measures: TOTAL_VIEW_TIME_SEC, TOTAL_AMOUNT_SPENT, VIEW_COUNT",
            "   ",
            "   Table 2: SEMANTIC_USER_METRICS", 
            "   - Purpose: Daily user-level aggregations",
            "   - Key Dimensions: USER_ID, COMPANY_ID, STATUS",
            "   - Time Dimensions: VIEW_DATE",
            "   - Measures: TOTAL_AMOUNT_SPENT, TOTAL_VIEW_TIME_SEC, DISTINCT_APP_COUNT, VIEW_COUNT",
            "   ",
            "   Table 3: SEMANTIC_COMPANY_METRICS",
            "   - Purpose: Daily company-level aggregations", 
            "   - Key Dimensions: COMPANY_ID, STATUS",
            "   - Time Dimensions: VIEW_DATE",
            "   - Measures: TOTAL_AMOUNT_SPENT, TOTAL_VIEW_TIME_SEC, DISTINCT_APP_COUNT, VIEW_COUNT, DISTINCT_USER_COUNT",
            
            "\n🔗 STEP 5: Define Relationships",
            "   Create these relationships between tables:",
            "   ",
            "   SEMANTIC_ANON_VIEWS → SEMANTIC_USER_METRICS:",
            "   - Join on: USER_ID = USER_ID AND VIEW_DATE = VIEW_DATE",
            "   ",
            "   SEMANTIC_ANON_VIEWS → SEMANTIC_COMPANY_METRICS:",
            "   - Join on: COMPANY_ID = COMPANY_ID AND VIEW_DATE = VIEW_DATE",
            "   ",
            "   SEMANTIC_USER_METRICS → SEMANTIC_COMPANY_METRICS:",
            "   - Join on: COMPANY_ID = COMPANY_ID AND VIEW_DATE = VIEW_DATE",
            
            "\n📏 STEP 6: Configure Dimensions",
            "   For each dimension, set these properties:",
            "   ",
            "   APP_ID:",
            "   - Type: Dimension",
            "   - Synonyms: app_identifier, application_id, app_key",
            "   ",
            "   USER_ID:",
            "   - Type: Dimension", 
            "   - Synonyms: user_identifier, viewer_id, customer_user_id",
            "   ",
            "   COMPANY_ID:",
            "   - Type: Dimension",
            "   - Synonyms: organization_id, account_id, customer_id",
            "   ",
            "   STATUS:",
            "   - Type: Dimension",
            "   - Synonyms: company_status, subscription_status, account_status",
            "   ",
            "   VIEW_DATE:",
            "   - Type: Time Dimension",
            "   - Synonyms: event_date, date, ds",
            
            "\n📊 STEP 7: Configure Measures",
            "   For each measure, set these properties:",
            "   ",
            "   TOTAL_AMOUNT_SPENT:",
            "   - Type: Measure",
            "   - Aggregation: SUM",
            "   - Synonyms: revenue, spend, total_charges",
            "   ",
            "   TOTAL_VIEW_TIME_SEC:",
            "   - Type: Measure",
            "   - Aggregation: SUM", 
            "   - Synonyms: watch_time, time_spent, view_duration",
            "   ",
            "   VIEW_COUNT:",
            "   - Type: Measure",
            "   - Aggregation: SUM",
            "   - Synonyms: views, page_views, impressions",
            "   ",
            "   DISTINCT_APP_COUNT:",
            "   - Type: Measure",
            "   - Aggregation: SUM",
            "   - Synonyms: unique_apps, distinct_apps",
            "   ",
            "   DISTINCT_USER_COUNT:",
            "   - Type: Measure", 
            "   - Aggregation: SUM",
            "   - Synonyms: unique_users, distinct_users",
            
            "\n✅ STEP 8: Save and Test",
            "   1. Click 'Save' to create your semantic model",
            "   2. Test with sample queries like:",
            "      - 'Show me total revenue by company status'",
            "      - 'What is the average view time per user?'",
            "      - 'How many unique apps were viewed last month?'",
            
            "\n🎉 STEP 9: You're Done!",
            "   Your semantic model is now ready for:",
            "   - Natural language queries",
            "   - Dashboard creation", 
            "   - Business intelligence analysis",
            "   - Data exploration"
        ]
        
        for instruction in instructions:
            print(instruction)
        
        print("\n" + "="*80)
    
    def close(self):
        """Close connection"""
        if self.connection:
            self.connection.close()
            logger.info("Closed Snowflake connection")

def main():
    """Main function"""
    print("🚀 Snowflake Semantic Model Creator")
    print("=" * 50)
    
    snowflake_config = SnowflakeConfig(
        account="jsovftw-uu47247",
        user="manideep285",
        password="v.Manideep@123",
        warehouse="COMPUTE_WH",
        database="ANON_APP_PLATFORM",
        schema="PUBLIC",
        role="ACCOUNTADMIN"
    )
    
    creator = SemanticModelCreator(snowflake_config)
    
    try:
        # Step 1: Execute preparation SQL
        creator.execute_preparation_sql()
        
        # Step 2: Verify readiness
        creator.verify_semantic_model_readiness()
        
        # Step 3: Generate Snowsight instructions
        creator.generate_snowsight_instructions()
        
        print(f"\n🎯 QUICK ACCESS LINK:")
        print(f"   https://{snowflake_config.account}.snowflakecomputing.com")
        print(f"   Username: {snowflake_config.user}")
        print(f"   Database: {snowflake_config.database}")
        print(f"   Schema: {snowflake_config.schema}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        creator.close()

if __name__ == "__main__":
    main()