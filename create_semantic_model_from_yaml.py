#!/usr/bin/env python3
"""
Semantic Model Creator from YAML
================================
This script creates a Snowflake semantic model based on the semantic_model.yaml file,
but corrected for your actual database and schema setup.
"""

import os
import sys
import yaml
import logging
from dataclasses import dataclass
from typing import Optional, Dict, List, Any

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

class SemanticModelFromYAML:
    """Creates semantic model infrastructure from YAML definition"""
    
    def __init__(self, snowflake_config: SnowflakeConfig, yaml_file_path: str):
        self.config = snowflake_config
        self.yaml_file_path = yaml_file_path
        self.connection = None
        self.semantic_model_data = None
        
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
    
    def load_semantic_model_yaml(self):
        """Load and parse the semantic model YAML file"""
        try:
            with open(self.yaml_file_path, 'r') as f:
                self.semantic_model_data = yaml.safe_load(f)
            logger.info(f"✅ Loaded semantic model from {self.yaml_file_path}")
            return True
        except Exception as e:
            logger.error(f"❌ Error loading YAML file: {e}")
            return False
    
    def create_semantic_views_from_yaml(self):
        """Create semantic views based on YAML definition"""
        if not self.connection:
            self.connect()
        
        if not self.semantic_model_data:
            logger.error("❌ No semantic model data loaded")
            return
        
        cursor = self.connection.cursor()
        
        try:
            print("🔧 Creating semantic views from YAML definition...")
            
            for table_def in self.semantic_model_data.get('tables', []):
                table_name = table_def.get('name', '').upper()
                
                # Map original table names to our actual table names
                actual_table_name = self._map_table_name(table_name)
                view_name = f"SEMANTIC_{actual_table_name}"
                
                # Generate view SQL
                view_sql = self._generate_view_sql(table_def, actual_table_name, view_name)
                
                if view_sql:
                    cursor.execute(view_sql)
                    logger.info(f"✅ Created semantic view: {view_name}")
                    print(f"✅ Created semantic view: {view_name}")
            
            print("✅ All semantic views created successfully!")
            
        except Exception as e:
            logger.error(f"❌ Error creating semantic views: {e}")
            raise
        finally:
            cursor.close()
    
    def _map_table_name(self, original_name: str) -> str:
        """Map original table names to actual table names"""
        mapping = {
            'ANON_VIEWS': 'ANON_VIEWS',
            'ANON_USER_DAY_FACT': 'ANON_USER_DAY_FACT',
            'ANON_COMPANY_DAY_FACT': 'ANON_COMPANY_DAY_FACT'
        }
        return mapping.get(original_name.upper(), original_name.upper())
    
    def _generate_view_sql(self, table_def: Dict, actual_table_name: str, view_name: str) -> str:
        """Generate CREATE VIEW SQL from table definition"""
        try:
            # Collect all columns from dimensions, time_dimensions, and facts
            columns = []
            
            # Add dimensions
            for dim in table_def.get('dimensions', []):
                col_name = dim.get('name')
                if col_name:
                    columns.append(col_name)
            
            # Add time dimensions
            for time_dim in table_def.get('time_dimensions', []):
                col_name = time_dim.get('name')
                if col_name:
                    columns.append(col_name)
            
            # Add facts/measures
            for fact in table_def.get('facts', []):
                col_name = fact.get('name')
                expr = fact.get('expr')
                
                if col_name and expr:
                    if expr == '1':  # Special case for VIEW_COUNT
                        columns.append(f"1 as {col_name}")
                    elif expr == col_name:  # Direct column reference
                        columns.append(col_name)
                    elif expr == 'TOTAL_VIEW_TIME':  # Fix for YAML inconsistency
                        columns.append(f"TOTAL_VIEW_TIME_SEC as {col_name}")
                    elif expr == 'APP_COUNT':  # Fix for YAML inconsistency
                        columns.append(f"DISTINCT_APP_COUNT as {col_name}")
                    elif expr == 'USER_COUNT':  # Fix for YAML inconsistency
                        columns.append(f"DISTINCT_USER_COUNT as {col_name}")
                    else:  # Expression or different column name
                        columns.append(f"{expr} as {col_name}")
            
            # Generate the CREATE VIEW statement
            columns_sql = ",\n    ".join(columns)
            
            view_sql = f"""
CREATE OR REPLACE VIEW {view_name} AS
SELECT
    {columns_sql}
FROM {actual_table_name}
"""
            
            return view_sql.strip()
            
        except Exception as e:
            logger.error(f"❌ Error generating view SQL for {view_name}: {e}")
            return None
    
    def create_semantic_model_documentation(self):
        """Create documentation for the semantic model"""
        if not self.semantic_model_data:
            return
        
        print("\n" + "="*80)
        print("📋 SEMANTIC MODEL DOCUMENTATION")
        print("="*80)
        
        model_name = self.semantic_model_data.get('name', 'Unknown')
        model_desc = self.semantic_model_data.get('description', 'No description')
        
        print(f"\n🏷️  Model Name: {model_name}")
        print(f"📝 Description: {model_desc}")
        
        print(f"\n📊 Tables and Structure:")
        
        for table_def in self.semantic_model_data.get('tables', []):
            table_name = table_def.get('name', '').upper()
            actual_table_name = self._map_table_name(table_name)
            view_name = f"SEMANTIC_{actual_table_name}"
            table_desc = table_def.get('description', 'No description')
            
            print(f"\n🗂️  Table: {view_name}")
            print(f"   Description: {table_desc}")
            
            # Dimensions
            dimensions = table_def.get('dimensions', [])
            if dimensions:
                print(f"   📏 Dimensions ({len(dimensions)}):")
                for dim in dimensions:
                    name = dim.get('name', 'Unknown')
                    desc = dim.get('description', 'No description')
                    synonyms = dim.get('synonyms', [])
                    print(f"     - {name}: {desc}")
                    if synonyms:
                        print(f"       Synonyms: {', '.join(synonyms)}")
            
            # Time Dimensions
            time_dims = table_def.get('time_dimensions', [])
            if time_dims:
                print(f"   ⏰ Time Dimensions ({len(time_dims)}):")
                for time_dim in time_dims:
                    name = time_dim.get('name', 'Unknown')
                    desc = time_dim.get('description', 'No description')
                    synonyms = time_dim.get('synonyms', [])
                    print(f"     - {name}: {desc}")
                    if synonyms:
                        print(f"       Synonyms: {', '.join(synonyms)}")
            
            # Facts/Measures
            facts = table_def.get('facts', [])
            if facts:
                print(f"   📊 Measures ({len(facts)}):")
                for fact in facts:
                    name = fact.get('name', 'Unknown')
                    desc = fact.get('description', 'No description')
                    synonyms = fact.get('synonyms', [])
                    agg = fact.get('default_aggregation', 'SUM')
                    print(f"     - {name}: {desc} (Aggregation: {agg})")
                    if synonyms:
                        print(f"       Synonyms: {', '.join(synonyms)}")
        
        print("\n" + "="*80)
    
    def generate_snowsight_creation_guide(self):
        """Generate step-by-step guide for creating semantic model in Snowsight"""
        if not self.semantic_model_data:
            return
        
        model_name = self.semantic_model_data.get('name', 'paid_app_platform_anon')
        model_desc = self.semantic_model_data.get('description', 'Usage data for a paid-app platform')
        
        print("\n" + "="*80)
        print("🎯 SNOWSIGHT SEMANTIC MODEL CREATION GUIDE")
        print("="*80)
        
        print(f"\n🌐 STEP 1: Access Snowsight")
        print(f"   URL: https://{self.config.account}.snowflakecomputing.com")
        print(f"   Username: {self.config.user}")
        print(f"   Database: {self.config.database}")
        print(f"   Schema: {self.config.schema}")
        
        print(f"\n🏗️ STEP 2: Create Semantic Model")
        print(f"   1. Navigate to Data > Semantic Models")
        print(f"   2. Click 'Create Semantic Model'")
        print(f"   3. Model Name: {model_name.upper()}")
        print(f"   4. Description: {model_desc}")
        
        print(f"\n📋 STEP 3: Add Base Tables")
        
        for i, table_def in enumerate(self.semantic_model_data.get('tables', []), 1):
            table_name = table_def.get('name', '').upper()
            actual_table_name = self._map_table_name(table_name)
            view_name = f"SEMANTIC_{actual_table_name}"
            table_desc = table_def.get('description', 'No description')
            
            print(f"\n   Table {i}: {view_name}")
            print(f"   Purpose: {table_desc}")
            
            # List dimensions
            dimensions = [dim.get('name') for dim in table_def.get('dimensions', [])]
            time_dims = [td.get('name') for td in table_def.get('time_dimensions', [])]
            facts = [fact.get('name') for fact in table_def.get('facts', [])]
            
            if dimensions:
                print(f"   Dimensions: {', '.join(dimensions)}")
            if time_dims:
                print(f"   Time Dimensions: {', '.join(time_dims)}")
            if facts:
                print(f"   Measures: {', '.join(facts)}")
        
        print(f"\n🔗 STEP 4: Define Relationships")
        print(f"   Connect tables using these common keys:")
        print(f"   - USER_ID (between ANON_VIEWS and USER_DAY_FACT)")
        print(f"   - COMPANY_ID (between all tables)")
        print(f"   - VIEW_DATE (for time-based relationships)")
        
        print(f"\n✅ STEP 5: Configure Synonyms")
        print(f"   Add synonyms for natural language queries:")
        
        # Show key synonyms
        for table_def in self.semantic_model_data.get('tables', []):
            for dim in table_def.get('dimensions', []):
                if dim.get('synonyms'):
                    print(f"   - {dim.get('name')}: {', '.join(dim.get('synonyms', [])[:3])}...")
            for fact in table_def.get('facts', []):
                if fact.get('synonyms'):
                    print(f"   - {fact.get('name')}: {', '.join(fact.get('synonyms', [])[:3])}...")
        
        print(f"\n🎉 STEP 6: Test Your Model")
        print(f"   Try these natural language queries:")
        print(f"   - 'Show me total revenue by company status'")
        print(f"   - 'What is the average view time per user?'")
        print(f"   - 'How many paying customers viewed apps last month?'")
        print(f"   - 'Which apps have the highest total view time?'")
        
        print("\n" + "="*80)
    
    def verify_setup(self):
        """Verify that all components are ready"""
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        
        print("\n🔍 SETUP VERIFICATION:")
        print("="*50)
        
        try:
            # Check base tables
            base_tables = ['ANON_VIEWS', 'ANON_USER_DAY_FACT', 'ANON_COMPANY_DAY_FACT']
            print("\n📋 Base Tables:")
            for table in base_tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"  ✅ {table}: {count:,} rows")
            
            # Check semantic views
            semantic_views = ['SEMANTIC_ANON_VIEWS', 'SEMANTIC_ANON_USER_DAY_FACT', 'SEMANTIC_ANON_COMPANY_DAY_FACT']
            print("\n🔍 Semantic Views:")
            for view in semantic_views:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {view}")
                    count = cursor.fetchone()[0]
                    print(f"  ✅ {view}: {count:,} rows")
                except:
                    print(f"  ❌ {view}: Not found (will be created)")
            
            print("\n✅ Setup verification complete!")
            
        except Exception as e:
            logger.error(f"❌ Error during verification: {e}")
        finally:
            cursor.close()
    
    def close(self):
        """Close connection"""
        if self.connection:
            self.connection.close()
            logger.info("Closed Snowflake connection")

def main():
    """Main function"""
    print("🚀 Semantic Model Creator from YAML")
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
    
    # Path to semantic model YAML
    yaml_file_path = "anon_app_platform_dataset/semantic_model.yaml"
    
    # Check if YAML file exists
    if not os.path.exists(yaml_file_path):
        print(f"❌ YAML file not found: {yaml_file_path}")
        print("Available files:")
        for file in os.listdir("."):
            if file.endswith('.yaml') or file.endswith('.yml'):
                print(f"  - {file}")
        return
    
    # Create semantic model manager
    creator = SemanticModelFromYAML(snowflake_config, yaml_file_path)
    
    try:
        # Step 1: Load YAML
        if not creator.load_semantic_model_yaml():
            return
        
        # Step 2: Verify current setup
        creator.verify_setup()
        
        # Step 3: Create semantic views
        creator.create_semantic_views_from_yaml()
        
        # Step 4: Generate documentation
        creator.create_semantic_model_documentation()
        
        # Step 5: Generate Snowsight guide
        creator.generate_snowsight_creation_guide()
        
        print(f"\n🎯 NEXT STEPS:")
        print(f"1. Your semantic views are now created in Snowflake")
        print(f"2. Follow the Snowsight guide above to create the semantic model")
        print(f"3. Use the documentation to configure dimensions and measures")
        print(f"4. Test with natural language queries")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        creator.close()

if __name__ == "__main__":
    main()