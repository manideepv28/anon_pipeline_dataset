#!/usr/bin/env python3
"""
Configuration File for Snowflake Semantic Model Pipeline
========================================================

EDIT THIS FILE WITH YOUR CREDENTIALS AND SETTINGS

This is the ONLY file you need to modify to run the pipeline.
All scripts will read from this configuration file.
"""

# =============================================================================
# SNOWFLAKE CONFIGURATION - EDIT THESE VALUES
# =============================================================================

# Your Snowflake account identifier (without .snowflakecomputing.com)
# Example: "abc12345" or "mycompany-account"
SNOWFLAKE_ACCOUNT = "jsovftw-uu47247"

# Your Snowflake username
SNOWFLAKE_USER = "manideep285"

# Your Snowflake password
SNOWFLAKE_PASSWORD = "v.Manideep@123"

# Snowflake warehouse name (must be running)
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"

# Database name (will be created if it doesn't exist)
SNOWFLAKE_DATABASE = "ANON_APP_PLATFORM"

# Schema name (will be created if it doesn't exist)
SNOWFLAKE_SCHEMA = "PUBLIC"

# Your Snowflake role
SNOWFLAKE_ROLE = "ACCOUNTADMIN"

# =============================================================================
# DATA CONFIGURATION - EDIT IF NEEDED
# =============================================================================

# Directory containing your split CSV files
# This should contain files like: anon_views_part_01.csv, anon_user_day_fact.csv, etc.
SPLIT_FILES_DIRECTORY = "split_files"

# Path to your semantic model YAML file
SEMANTIC_MODEL_YAML = "semantic_model.yaml"

# GitHub repository URL (if fetching from GitHub)
GITHUB_REPO_URL = "https://github.com/sfc-gh-trichards/anon_app_platform_dataset"

# GitHub token (optional, for private repos)
GITHUB_TOKEN = None

# =============================================================================
# PIPELINE CONFIGURATION - USUALLY DON'T NEED TO CHANGE
# =============================================================================

# Table names mapping (CSV filename -> Snowflake table name)
TABLE_MAPPINGS = {
    'anon_views': 'ANON_VIEWS',
    'anon_user_day_fact': 'ANON_USER_DAY_FACT', 
    'anon_company_day_fact': 'ANON_COMPANY_DAY_FACT'
}

# Column mappings for CSV files (handles different column names)
COLUMN_MAPPINGS = {
    'ANON_VIEWS': {
        'app_id': 'APP_ID',
        'user_id': 'USER_ID',
        'company_id': 'COMPANY_ID',
        'status': 'STATUS',
        'company_status': 'STATUS',  # Handle both variations
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

# Logging configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

# =============================================================================
# VALIDATION FUNCTIONS - DON'T EDIT
# =============================================================================

def validate_config():
    """Validate configuration settings"""
    errors = []
    
    # Check required Snowflake settings
    if not SNOWFLAKE_ACCOUNT:
        errors.append("SNOWFLAKE_ACCOUNT is required")
    if not SNOWFLAKE_USER:
        errors.append("SNOWFLAKE_USER is required")
    if not SNOWFLAKE_PASSWORD:
        errors.append("SNOWFLAKE_PASSWORD is required")
    if not SNOWFLAKE_DATABASE:
        errors.append("SNOWFLAKE_DATABASE is required")
    
    # Check file paths
    import os
    if not os.path.exists(SPLIT_FILES_DIRECTORY):
        errors.append(f"Split files directory not found: {SPLIT_FILES_DIRECTORY}")
    
    if SEMANTIC_MODEL_YAML and not os.path.exists(SEMANTIC_MODEL_YAML):
        errors.append(f"Semantic model YAML not found: {SEMANTIC_MODEL_YAML}")
    
    return errors

def get_snowflake_config():
    """Get Snowflake configuration as a dictionary"""
    return {
        'account': SNOWFLAKE_ACCOUNT,
        'user': SNOWFLAKE_USER,
        'password': SNOWFLAKE_PASSWORD,
        'warehouse': SNOWFLAKE_WAREHOUSE,
        'database': SNOWFLAKE_DATABASE,
        'schema': SNOWFLAKE_SCHEMA,
        'role': SNOWFLAKE_ROLE
    }

def print_config_summary():
    """Print configuration summary (without sensitive data)"""
    print("📋 Configuration Summary:")
    print(f"   Snowflake Account: {SNOWFLAKE_ACCOUNT}")
    print(f"   Username: {SNOWFLAKE_USER}")
    print(f"   Database: {SNOWFLAKE_DATABASE}")
    print(f"   Schema: {SNOWFLAKE_SCHEMA}")
    print(f"   Warehouse: {SNOWFLAKE_WAREHOUSE}")
    print(f"   Split Files Directory: {SPLIT_FILES_DIRECTORY}")
    print(f"   Semantic Model YAML: {SEMANTIC_MODEL_YAML}")

if __name__ == "__main__":
    # Test configuration when run directly
    print("🔧 Testing Configuration...")
    print_config_summary()
    
    errors = validate_config()
    if errors:
        print("\n❌ Configuration Errors:")
        for error in errors:
            print(f"   - {error}")
    else:
        print("\n✅ Configuration is valid!")