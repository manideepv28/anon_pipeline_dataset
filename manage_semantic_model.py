#!/usr/bin/env python3
"""
Semantic Model Manager for Snowflake
====================================
This script helps you manage semantic model files in Snowflake stages
and provides SQL commands to work with them.
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

class SemanticModelManager:
    """Manages semantic model files and provides SQL commands"""
    
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
    
    def show_stages_and_files(self):
        """Show all stages and their files"""
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        
        try:
            print("\n📁 AVAILABLE STAGES:")
            print("="*50)
            
            cursor.execute("SHOW STAGES")
            stages = cursor.fetchall()
            
            for stage in stages:
                stage_name = stage[1]
                print(f"\n🗂️  Stage: {stage_name}")
                
                try:
                    cursor.execute(f"LIST @{stage_name}")
                    files = cursor.fetchall()
                    
                    if files:
                        print("   Files:")
                        for file_info in files:
                            file_name = file_info[0]
                            file_size = file_info[1]
                            print(f"   📄 {file_name} ({file_size} bytes)")
                    else:
                        print("   (No files)")
                        
                except Exception as e:
                    print(f"   ❌ Error listing files: {e}")
            
        except Exception as e:
            logger.error(f"Error showing stages: {e}")
        finally:
            cursor.close()
    
    def create_semantic_model_sql_commands(self):
        """Generate SQL commands for creating semantic models"""
        
        print("\n🔧 SQL COMMANDS FOR SEMANTIC MODEL CREATION:")
        print("="*60)
        
        # Based on your semantic_model.yaml structure
        sql_commands = [
            "-- Step 1: Use your database and schema",
            f"USE DATABASE {self.config.database};",
            f"USE SCHEMA {self.config.schema};",
            "",
            "-- Step 2: Create semantic model using your existing tables",
            "-- Note: Snowflake semantic models are created through the web UI or Snowsight",
            "-- Here are the table references you can use:",
            "",
            "-- Your tables are already created:",
            "-- ANON_VIEWS (2.5M rows)",
            "-- ANON_USER_DAY_FACT (722K rows)", 
            "-- ANON_COMPANY_DAY_FACT (331K rows)",
            "",
            "-- Step 3: Verify your data is ready for semantic modeling",
            "SELECT 'ANON_VIEWS' as table_name, COUNT(*) as row_count FROM ANON_VIEWS",
            "UNION ALL",
            "SELECT 'ANON_USER_DAY_FACT', COUNT(*) FROM ANON_USER_DAY_FACT",
            "UNION ALL", 
            "SELECT 'ANON_COMPANY_DAY_FACT', COUNT(*) FROM ANON_COMPANY_DAY_FACT;",
            "",
            "-- Step 4: Sample your data to understand structure",
            "SELECT * FROM ANON_VIEWS LIMIT 5;",
            "SELECT * FROM ANON_USER_DAY_FACT LIMIT 5;",
            "SELECT * FROM ANON_COMPANY_DAY_FACT LIMIT 5;",
            "",
            "-- Step 5: Create views for semantic modeling (if needed)",
            "-- These views can be used as base tables for semantic models",
            "",
            "CREATE OR REPLACE VIEW SEMANTIC_ANON_VIEWS AS",
            "SELECT ",
            "    APP_ID,",
            "    USER_ID,", 
            "    COMPANY_ID,",
            "    STATUS,",
            "    VIEW_TIME,",
            "    VIEW_DATE,",
            "    TOTAL_VIEW_TIME_SEC,",
            "    TOTAL_AMOUNT_SPENT,",
            "    1 as VIEW_COUNT",
            "FROM ANON_VIEWS;",
            "",
            "CREATE OR REPLACE VIEW SEMANTIC_USER_METRICS AS",
            "SELECT",
            "    USER_ID,",
            "    COMPANY_ID,",
            "    STATUS,",
            "    VIEW_DATE,",
            "    TOTAL_AMOUNT_SPENT,",
            "    TOTAL_VIEW_TIME_SEC,",
            "    DISTINCT_APP_COUNT,",
            "    VIEW_COUNT",
            "FROM ANON_USER_DAY_FACT;",
            "",
            "CREATE OR REPLACE VIEW SEMANTIC_COMPANY_METRICS AS",
            "SELECT",
            "    COMPANY_ID,",
            "    STATUS,",
            "    VIEW_DATE,",
            "    TOTAL_AMOUNT_SPENT,",
            "    TOTAL_VIEW_TIME_SEC,",
            "    DISTINCT_APP_COUNT,",
            "    VIEW_COUNT,",
            "    DISTINCT_USER_COUNT",
            "FROM ANON_COMPANY_DAY_FACT;"
        ]
        
        for cmd in sql_commands:
            print(cmd)
        
        print("\n" + "="*60)
        print("📋 NEXT STEPS FOR SEMANTIC MODEL CREATION:")
        print("="*60)
        print("1. Copy the SQL commands above and run them in Snowflake")
        print("2. Go to Snowsight (Snowflake Web UI)")
        print("3. Navigate to 'Data' > 'Semantic Models'")
        print("4. Click 'Create Semantic Model'")
        print("5. Use your tables as base tables:")
        print("   - ANON_VIEWS or SEMANTIC_ANON_VIEWS")
        print("   - ANON_USER_DAY_FACT or SEMANTIC_USER_METRICS")
        print("   - ANON_COMPANY_DAY_FACT or SEMANTIC_COMPANY_METRICS")
        print("6. Define dimensions, measures, and relationships")
        print("="*60)
    
    def create_semantic_views(self):
        """Create views optimized for semantic modeling"""
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        
        views_sql = [
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
            for i, sql in enumerate(views_sql, 1):
                cursor.execute(sql)
                view_name = sql.split("VIEW ")[1].split(" AS")[0]
                logger.info(f"✅ Created view {i}/3: {view_name}")
            
            print("\n✅ All semantic views created successfully!")
            
        except Exception as e:
            logger.error(f"❌ Error creating views: {e}")
        finally:
            cursor.close()
    
    def show_semantic_model_info(self):
        """Show information about semantic model setup"""
        print("\n📊 SEMANTIC MODEL SETUP STATUS:")
        print("="*50)
        
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        
        try:
            # Check tables
            tables = ['ANON_VIEWS', 'ANON_USER_DAY_FACT', 'ANON_COMPANY_DAY_FACT']
            print("\n📋 Base Tables:")
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    print(f"  ✅ {table}: {count:,} rows")
                except:
                    print(f"  ❌ {table}: Not found")
            
            # Check semantic views
            views = ['SEMANTIC_ANON_VIEWS', 'SEMANTIC_USER_METRICS', 'SEMANTIC_COMPANY_METRICS']
            print("\n🔍 Semantic Views:")
            for view in views:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {view}")
                    count = cursor.fetchone()[0]
                    print(f"  ✅ {view}: {count:,} rows")
                except:
                    print(f"  ❌ {view}: Not created yet")
            
            # Check stages
            print("\n📁 Stages:")
            try:
                cursor.execute("SHOW STAGES")
                stages = cursor.fetchall()
                for stage in stages:
                    print(f"  ✅ {stage[1]}")
            except:
                print("  ❌ No stages found")
                
        except Exception as e:
            logger.error(f"Error checking status: {e}")
        finally:
            cursor.close()
    
    def close(self):
        """Close connection"""
        if self.connection:
            self.connection.close()

def main():
    """Main function"""
    print("🔧 Semantic Model Manager")
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
    
    manager = SemanticModelManager(snowflake_config)
    
    try:
        # Show current status
        manager.show_semantic_model_info()
        
        # Show stages and files
        manager.show_stages_and_files()
        
        # Ask if user wants to create semantic views
        response = input("\nDo you want to create semantic views for modeling? (y/N): ").lower().strip()
        if response in ['y', 'yes']:
            print("\n🔧 Creating semantic views...")
            manager.create_semantic_views()
        
        # Generate SQL commands
        manager.create_semantic_model_sql_commands()
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        manager.close()

if __name__ == "__main__":
    main()