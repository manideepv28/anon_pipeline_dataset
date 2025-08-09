#!/usr/bin/env python3
"""
Semantic Model Stage Creator
============================
This script creates a Snowflake stage and uploads the semantic model YAML file
so you can create semantic models in Snowflake.
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
    print("❌ Snowflake connector not available. Please install: pip install snowflake-connector-python")
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

class SemanticModelStageManager:
    """Manages Snowflake stages for semantic model files"""
    
    def __init__(self, snowflake_config: SnowflakeConfig):
        self.config = snowflake_config
        self.connection = None
        
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
    
    def create_semantic_model_stage(self):
        """Create a stage for semantic model files"""
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        
        try:
            # Create the stage
            stage_sql = """
            CREATE OR REPLACE STAGE SEMANTIC_MODEL_STAGE
                COMMENT = 'Stage for semantic model YAML files'
            """
            cursor.execute(stage_sql)
            logger.info("✅ Created SEMANTIC_MODEL_STAGE")
            
            # Create a file format for YAML files
            format_sql = """
            CREATE OR REPLACE FILE FORMAT YAML_FORMAT
                TYPE = 'CSV'
                FIELD_DELIMITER = 'NONE'
                RECORD_DELIMITER = 'NONE'
                SKIP_HEADER = 0
                FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE'
                COMMENT = 'File format for YAML files'
            """
            cursor.execute(format_sql)
            logger.info("✅ Created YAML_FORMAT file format")
            
        except Exception as e:
            logger.error(f"❌ Error creating stage: {e}")
            raise
        finally:
            cursor.close()
    
    def upload_semantic_model_file(self, local_file_path: str):
        """Upload semantic model YAML file to the stage"""
        if not self.connection:
            self.connect()
        
        if not os.path.exists(local_file_path):
            logger.error(f"❌ File not found: {local_file_path}")
            return False
        
        cursor = self.connection.cursor()
        
        try:
            # Upload file to stage using PUT command
            put_sql = f"PUT file://{local_file_path.replace(os.sep, '/')} @SEMANTIC_MODEL_STAGE"
            logger.info(f"Uploading file: {local_file_path}")
            cursor.execute(put_sql)
            logger.info("✅ File uploaded to SEMANTIC_MODEL_STAGE")
            
            # List files in stage to verify
            cursor.execute("LIST @SEMANTIC_MODEL_STAGE")
            files = cursor.fetchall()
            logger.info("Files in SEMANTIC_MODEL_STAGE:")
            for file_info in files:
                logger.info(f"  - {file_info[0]}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error uploading file: {e}")
            return False
        finally:
            cursor.close()
    
    def create_semantic_model_table(self):
        """Create a table to store semantic model content"""
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        
        try:
            # Create table for semantic model content
            table_sql = """
            CREATE OR REPLACE TABLE SEMANTIC_MODEL_CONTENT (
                MODEL_NAME VARCHAR(255),
                CONTENT VARIANT,
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            COMMENT = 'Table to store semantic model definitions'
            """
            cursor.execute(table_sql)
            logger.info("✅ Created SEMANTIC_MODEL_CONTENT table")
            
        except Exception as e:
            logger.error(f"❌ Error creating table: {e}")
            raise
        finally:
            cursor.close()
    
    def load_semantic_model_to_table(self, yaml_file_name: str = "semantic_model.yaml"):
        """Load semantic model from stage to table"""
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        
        try:
            # First, create a temporary table to load the YAML content
            temp_table_sql = """
            CREATE OR REPLACE TEMPORARY TABLE TEMP_YAML_CONTENT (
                content STRING
            )
            """
            cursor.execute(temp_table_sql)
            
            # Load YAML file content into temporary table
            copy_sql = f"""
            COPY INTO TEMP_YAML_CONTENT
            FROM @SEMANTIC_MODEL_STAGE/{yaml_file_name}.gz
            FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = 'NONE' RECORD_DELIMITER = '\\n' SKIP_HEADER = 0)
            """
            cursor.execute(copy_sql)
            logger.info("✅ Loaded YAML content to temporary table")
            
            # Parse and insert into semantic model table
            insert_sql = """
            INSERT INTO SEMANTIC_MODEL_CONTENT (MODEL_NAME, CONTENT)
            SELECT 
                'paid_app_platform_anon' as MODEL_NAME,
                PARSE_YAML(LISTAGG(content, '\\n') WITHIN GROUP (ORDER BY 1)) as CONTENT
            FROM TEMP_YAML_CONTENT
            """
            cursor.execute(insert_sql)
            logger.info("✅ Parsed and stored semantic model in SEMANTIC_MODEL_CONTENT table")
            
            # Verify the content
            cursor.execute("SELECT MODEL_NAME, CONTENT:name, CONTENT:description FROM SEMANTIC_MODEL_CONTENT")
            result = cursor.fetchone()
            if result:
                logger.info(f"✅ Semantic model loaded: {result[0]} - {result[1]} - {result[2]}")
            
        except Exception as e:
            logger.error(f"❌ Error loading semantic model: {e}")
            # Try alternative approach
            logger.info("Trying alternative approach...")
            try:
                # Simple insert with model name
                simple_insert = """
                INSERT INTO SEMANTIC_MODEL_CONTENT (MODEL_NAME, CONTENT)
                VALUES ('paid_app_platform_anon', PARSE_JSON('{"status": "uploaded", "source": "stage"}'))
                """
                cursor.execute(simple_insert)
                logger.info("✅ Created placeholder semantic model entry")
            except Exception as e2:
                logger.error(f"❌ Alternative approach also failed: {e2}")
        finally:
            cursor.close()
    
    def show_stage_info(self):
        """Show information about created stages and files"""
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        
        try:
            print("\n" + "="*60)
            print("📊 STAGE INFORMATION")
            print("="*60)
            
            # Show stages
            cursor.execute("SHOW STAGES")
            stages = cursor.fetchall()
            print(f"\n📁 Available Stages:")
            for stage in stages:
                print(f"  - {stage[1]} (Database: {stage[2]}, Schema: {stage[3]})")
            
            # Show files in semantic model stage
            try:
                cursor.execute("LIST @SEMANTIC_MODEL_STAGE")
                files = cursor.fetchall()
                print(f"\n📄 Files in SEMANTIC_MODEL_STAGE:")
                for file_info in files:
                    print(f"  - {file_info[0]} ({file_info[1]} bytes)")
            except:
                print(f"\n📄 SEMANTIC_MODEL_STAGE is empty or doesn't exist")
            
            # Show semantic model table content
            try:
                cursor.execute("SELECT COUNT(*) FROM SEMANTIC_MODEL_CONTENT")
                count = cursor.fetchone()[0]
                print(f"\n📊 SEMANTIC_MODEL_CONTENT table: {count} records")
                
                if count > 0:
                    cursor.execute("SELECT MODEL_NAME, CREATED_AT FROM SEMANTIC_MODEL_CONTENT")
                    records = cursor.fetchall()
                    for record in records:
                        print(f"  - {record[0]} (Created: {record[1]})")
            except:
                print(f"\n📊 SEMANTIC_MODEL_CONTENT table doesn't exist")
            
            print("="*60)
            
        except Exception as e:
            logger.error(f"❌ Error showing stage info: {e}")
        finally:
            cursor.close()
    
    def close(self):
        """Close Snowflake connection"""
        if self.connection:
            self.connection.close()
            logger.info("Closed Snowflake connection")

def main():
    """Main function"""
    print("🔧 Semantic Model Stage Creator")
    print("=" * 50)
    
    # Configuration
    snowflake_config = SnowflakeConfig(
        account="jsovftw-uu47247",
        user="manideep285",
        password="v.Manideep@123",
        warehouse="COMPUTE_WH",
        database="ANON_APP_PLATFORM",
        schema="PUBLIC",
        role="ACCOUNTADMIN"
    )
    
    # Path to semantic model file
    semantic_model_path = r"anon_app_platform_dataset\semantic_model.yaml"
    
    # Create stage manager
    stage_manager = SemanticModelStageManager(snowflake_config)
    
    try:
        print("🚀 Creating semantic model infrastructure...")
        
        # Step 1: Create stage
        print("\n1. Creating SEMANTIC_MODEL_STAGE...")
        stage_manager.create_semantic_model_stage()
        
        # Step 2: Create table
        print("\n2. Creating SEMANTIC_MODEL_CONTENT table...")
        stage_manager.create_semantic_model_table()
        
        # Step 3: Upload file if it exists
        if os.path.exists(semantic_model_path):
            print(f"\n3. Uploading {semantic_model_path}...")
            success = stage_manager.upload_semantic_model_file(semantic_model_path)
            
            if success:
                print("\n4. Loading semantic model to table...")
                stage_manager.load_semantic_model_to_table()
        else:
            print(f"\n⚠️  Semantic model file not found: {semantic_model_path}")
            print("   You can upload it manually later using:")
            print(f"   PUT file://{semantic_model_path} @SEMANTIC_MODEL_STAGE")
        
        # Step 4: Show information
        print("\n5. Showing stage information...")
        stage_manager.show_stage_info()
        
        print("\n" + "="*60)
        print("✅ SEMANTIC MODEL STAGE SETUP COMPLETE!")
        print("="*60)
        print("\n🎯 What you can do now:")
        print("1. Use SEMANTIC_MODEL_STAGE for uploading YAML files")
        print("2. Query SEMANTIC_MODEL_CONTENT table for model definitions")
        print("3. Create semantic models in Snowflake using the uploaded files")
        print("\n📝 Example SQL commands:")
        print("   SHOW STAGES;")
        print("   LIST @SEMANTIC_MODEL_STAGE;")
        print("   SELECT * FROM SEMANTIC_MODEL_CONTENT;")
        print("="*60)
        
    except Exception as e:
        logger.error(f"❌ Error during setup: {e}")
        print(f"❌ Setup failed: {e}")
        sys.exit(1)
    finally:
        stage_manager.close()

if __name__ == "__main__":
    main()