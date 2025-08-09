#!/usr/bin/env python3
"""
One-Command Snowflake Semantic Model Pipeline Setup
===================================================

This script runs the complete pipeline to set up your Snowflake semantic model.

Usage:
    python setup_pipeline.py

Prerequisites:
    1. Edit config.py with your Snowflake credentials
    2. Place your split CSV files in the split_files directory
    3. Ensure semantic_model.yaml is in the root directory
"""

import os
import sys
import subprocess
import logging
from datetime import datetime

# Import configuration
try:
    import config
except ImportError:
    print("❌ config.py not found. Please create it from the template.")
    sys.exit(1)

# Setup logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)

class PipelineRunner:
    """Runs the complete semantic model pipeline"""
    
    def __init__(self):
        self.steps_completed = 0
        self.total_steps = 7
        
    def print_header(self):
        """Print pipeline header"""
        print("🚀 Snowflake Semantic Model Pipeline")
        print("=" * 60)
        print("This will set up your complete semantic model infrastructure.")
        print("=" * 60)
        
    def print_step(self, step_num: int, description: str):
        """Print current step"""
        print(f"\n📋 STEP {step_num}/{self.total_steps}: {description}")
        print("-" * 50)
        
    def validate_prerequisites(self):
        """Validate all prerequisites"""
        self.print_step(1, "Validating Prerequisites")
        
        # Validate configuration
        config.print_config_summary()
        errors = config.validate_config()
        
        if errors:
            print("\n❌ Configuration Errors:")
            for error in errors:
                print(f"   - {error}")
            print("\n🔧 Please fix these errors in config.py and try again.")
            return False
        
        print("✅ Configuration is valid!")
        self.steps_completed += 1
        return True
    
    def install_requirements(self):
        """Install required Python packages"""
        self.print_step(2, "Installing Requirements")
        
        try:
            # Check if requirements.txt exists
            if not os.path.exists('requirements.txt'):
                print("⚠️  requirements.txt not found, installing core packages...")
                packages = ['snowflake-connector-python', 'PyYAML', 'requests', 'pandas']
                for package in packages:
                    subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            else:
                print("📦 Installing packages from requirements.txt...")
                subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
            
            print("✅ All packages installed successfully!")
            self.steps_completed += 1
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to install packages: {e}")
            return False
    
    def check_split_files(self):
        """Check split files"""
        self.print_step(3, "Checking Split Files")
        
        try:
            # Import and run the check script
            sys.path.append('.')
            from check_split_files import check_split_files
            check_split_files()
            
            print("✅ Split files check completed!")
            self.steps_completed += 1
            return True
            
        except Exception as e:
            print(f"❌ Error checking split files: {e}")
            return False
    
    def load_data(self):
        """Load split files into Snowflake"""
        self.print_step(4, "Loading Data into Snowflake")
        
        try:
            # Import the data loader
            from load_split_files import SplitFileLoader
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
            
            # Create config object
            snowflake_config = SnowflakeConfig(**config.get_snowflake_config())
            
            # Create and run loader
            loader = SplitFileLoader(snowflake_config, config.SPLIT_FILES_DIRECTORY)
            
            print("🔄 Loading data (this may take several minutes)...")
            results = loader.load_all_data(clear_existing=True)
            
            # Display results
            print("\n📊 Data Loading Results:")
            total_rows = 0
            for table, count in results.items():
                print(f"  - {table}: {count:,} rows loaded")
                total_rows += count
            
            print(f"\n✅ Total rows loaded: {total_rows:,}")
            loader.close()
            
            self.steps_completed += 1
            return True
            
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            logger.error(f"Data loading error: {e}")
            return False
    
    def verify_data_integrity(self):
        """Verify data integrity"""
        self.print_step(5, "Verifying Data Integrity")
        
        try:
            from verify_data_integrity import DataIntegrityChecker
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
            
            snowflake_config = SnowflakeConfig(**config.get_snowflake_config())
            checker = DataIntegrityChecker(config.SPLIT_FILES_DIRECTORY, snowflake_config)
            
            results = checker.run_full_verification()
            
            # Display results
            print("\n📊 Data Integrity Results:")
            perfect_tables = 0
            for table in results.get('local_counts', {}):
                local = results['local_counts'][table]
                snowflake = results['snowflake_counts'].get(table, -1)
                status = results['integrity_status'].get(table, 'UNKNOWN')
                duplicates = results['duplicate_counts'].get(table, -1)
                
                print(f"\n  {table}:")
                print(f"    Local files: {local:,} rows")
                print(f"    Snowflake: {snowflake:,} rows")
                print(f"    Duplicates: {duplicates}")
                print(f"    Status: {status}")
                
                if status == 'PERFECT':
                    perfect_tables += 1
            
            total_tables = len(results.get('integrity_status', {}))
            print(f"\n✅ {perfect_tables}/{total_tables} tables have perfect integrity")
            
            checker.close()
            self.steps_completed += 1
            return True
            
        except Exception as e:
            print(f"❌ Error verifying data: {e}")
            logger.error(f"Data verification error: {e}")
            return False
    
    def create_semantic_model(self):
        """Create semantic model infrastructure"""
        self.print_step(6, "Creating Semantic Model Infrastructure")
        
        try:
            from create_semantic_model_from_yaml import SemanticModelFromYAML
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
            
            snowflake_config = SnowflakeConfig(**config.get_snowflake_config())
            creator = SemanticModelFromYAML(snowflake_config, config.SEMANTIC_MODEL_YAML)
            
            # Load YAML and create views
            if creator.load_semantic_model_yaml():
                creator.create_semantic_views_from_yaml()
                print("✅ Semantic model infrastructure created!")
            else:
                print("❌ Failed to load semantic model YAML")
                return False
            
            creator.close()
            self.steps_completed += 1
            return True
            
        except Exception as e:
            print(f"❌ Error creating semantic model: {e}")
            logger.error(f"Semantic model creation error: {e}")
            return False
    
    def generate_final_instructions(self):
        """Generate final setup instructions"""
        self.print_step(7, "Generating Final Instructions")
        
        try:
            print("\n🎯 SNOWSIGHT SETUP INSTRUCTIONS:")
            print("=" * 60)
            
            snowflake_config = config.get_snowflake_config()
            
            print(f"\n🌐 Access Snowsight:")
            print(f"   URL: https://{snowflake_config['account']}.snowflakecomputing.com")
            print(f"   Username: {snowflake_config['user']}")
            print(f"   Database: {snowflake_config['database']}")
            print(f"   Schema: {snowflake_config['schema']}")
            
            print(f"\n📋 Create Semantic Model:")
            print(f"   1. Navigate to Data > Semantic Models")
            print(f"   2. Click 'Create Semantic Model'")
            print(f"   3. Model Name: PAID_APP_PLATFORM_ANON")
            print(f"   4. Add these base tables:")
            print(f"      - SEMANTIC_ANON_VIEWS")
            print(f"      - SEMANTIC_ANON_USER_DAY_FACT")
            print(f"      - SEMANTIC_ANON_COMPANY_DAY_FACT")
            
            print(f"\n🔗 Configure Relationships:")
            print(f"   - Connect tables using USER_ID, COMPANY_ID, and VIEW_DATE")
            
            print(f"\n🎉 Test Natural Language Queries:")
            print(f"   - 'Show me total revenue by company status'")
            print(f"   - 'What is the average view time per user?'")
            print(f"   - 'How many paying customers viewed apps last month?'")
            
            self.steps_completed += 1
            return True
            
        except Exception as e:
            print(f"❌ Error generating instructions: {e}")
            return False
    
    def run_pipeline(self):
        """Run the complete pipeline"""
        self.print_header()
        
        # Run all steps
        steps = [
            self.validate_prerequisites,
            self.install_requirements,
            self.check_split_files,
            self.load_data,
            self.verify_data_integrity,
            self.create_semantic_model,
            self.generate_final_instructions
        ]
        
        for step_func in steps:
            if not step_func():
                print(f"\n❌ Pipeline failed at step {self.steps_completed + 1}")
                print("Check the logs for detailed error information.")
                return False
        
        # Success summary
        print("\n" + "=" * 80)
        print("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 80)
        print(f"✅ All {self.total_steps} steps completed successfully")
        print("✅ Data loaded into Snowflake")
        print("✅ Semantic model infrastructure created")
        print("✅ Ready for Snowsight setup")
        print("\n🎯 Next: Follow the Snowsight instructions above to complete your semantic model!")
        print("=" * 80)
        
        return True

def main():
    """Main function"""
    # Check if config exists
    if not os.path.exists('config.py'):
        print("❌ config.py not found!")
        print("Please create config.py with your Snowflake credentials.")
        print("See the README.md for instructions.")
        return
    
    # Run pipeline
    runner = PipelineRunner()
    success = runner.run_pipeline()
    
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()