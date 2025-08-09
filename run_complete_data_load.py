#!/usr/bin/env python3
"""
Complete Data Loading Pipeline
==============================
This script runs the complete data loading process:
1. Loads all split files into Snowflake
2. Verifies data integrity
3. Provides summary report
"""

import sys
import subprocess
import os

def run_script(script_name):
    """Run a Python script and return success status"""
    try:
        print(f"\n🚀 Running {script_name}...")
        print("=" * 60)
        
        result = subprocess.run([sys.executable, script_name], 
                              capture_output=False, 
                              text=True)
        
        if result.returncode == 0:
            print(f"✅ {script_name} completed successfully")
            return True
        else:
            print(f"❌ {script_name} failed with return code {result.returncode}")
            return False
            
    except Exception as e:
        print(f"❌ Error running {script_name}: {e}")
        return False

def main():
    """Main pipeline function"""
    print("🔄 Complete Data Loading Pipeline")
    print("=" * 60)
    print("This will:")
    print("1. Load all split CSV files into Snowflake tables")
    print("2. Verify data integrity")
    print("3. Provide a summary report")
    print("=" * 60)
    
    # Check if required files exist
    required_files = ['load_split_files.py', 'verify_data_integrity.py']
    missing_files = [f for f in required_files if not os.path.exists(f)]
    
    if missing_files:
        print(f"❌ Missing required files: {', '.join(missing_files)}")
        return
    
    # Ask for confirmation
    response = input("\nDo you want to proceed? (y/N): ").lower().strip()
    if response not in ['y', 'yes']:
        print("Operation cancelled.")
        return
    
    success_count = 0
    total_steps = 2
    
    # Step 1: Load data
    if run_script('load_split_files.py'):
        success_count += 1
    
    # Step 2: Verify integrity
    if run_script('verify_data_integrity.py'):
        success_count += 1
    
    # Final summary
    print("\n" + "=" * 60)
    print("🎯 PIPELINE SUMMARY")
    print("=" * 60)
    
    if success_count == total_steps:
        print("✅ All steps completed successfully!")
        print("\n📊 Your data is now loaded in Snowflake:")
        print("  - Database: ANON_APP_PLATFORM")
        print("  - Schema: PUBLIC")
        print("  - Tables: ANON_VIEWS, ANON_USER_DAY_FACT, ANON_COMPANY_DAY_FACT")
        print("\n🎯 You can now run queries in Snowflake!")
    else:
        print(f"⚠️  {success_count}/{total_steps} steps completed successfully")
        print("Please check the error messages above and retry failed steps.")
    
    print("=" * 60)

if __name__ == "__main__":
    main()