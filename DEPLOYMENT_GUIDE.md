# 🚀 Deployment Guide: Snowflake Semantic Model Pipeline

This guide helps you deploy the complete Snowflake semantic model pipeline for a new user or environment.

## 📋 Prerequisites Checklist

Before starting, ensure you have:

- [ ] **Snowflake Account** with admin access
- [ ] **Python 3.7+** installed
- [ ] **Split CSV files** ready for upload
- [ ] **Semantic model YAML** file
- [ ] **Network access** to Snowflake from your machine

## 🎯 One-Command Deployment

### Step 1: Download/Clone Files

Create a new directory and copy these essential files:

```
your-project/
├── config.py                    # ← EDIT THIS FILE
├── setup_pipeline.py            # ← RUN THIS SCRIPT
├── requirements.txt
├── README.md
├── semantic_model.yaml          # Your semantic model definition
├── split_files/                 # Your CSV files directory
│   ├── anon_views_part_01.csv
│   ├── anon_views_part_02.csv
│   ├── anon_user_day_fact.csv
│   └── anon_company_day_fact.csv
└── scripts/                     # Supporting scripts
    ├── load_split_files.py
    ├── verify_data_integrity.py
    ├── create_semantic_model_from_yaml.py
    └── check_split_files.py
```

### Step 2: Configure Credentials

Edit `config.py` and update ONLY these values:

```python
# EDIT THESE VALUES FOR YOUR ENVIRONMENT
SNOWFLAKE_ACCOUNT = "your-account-id"        # e.g., "abc12345"
SNOWFLAKE_USER = "your-username"             # e.g., "john_doe"  
SNOWFLAKE_PASSWORD = "your-password"         # e.g., "MyPassword123"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"           # Your warehouse name
SNOWFLAKE_DATABASE = "YOUR_DATABASE"         # e.g., "ANALYTICS_DB"
SNOWFLAKE_SCHEMA = "PUBLIC"                  # Schema name
SNOWFLAKE_ROLE = "ACCOUNTADMIN"              # Your role

# UPDATE IF YOUR FILES ARE IN DIFFERENT LOCATIONS
SPLIT_FILES_DIRECTORY = "split_files"        # Directory with CSV files
SEMANTIC_MODEL_YAML = "semantic_model.yaml"  # Path to YAML file
```

### Step 3: Run One Command

```bash
python setup_pipeline.py
```

**That's it!** The script will:
- ✅ Validate your configuration
- ✅ Install required Python packages
- ✅ Check your split files
- ✅ Connect to Snowflake
- ✅ Create database and schema
- ✅ Load all CSV data
- ✅ Verify data integrity
- ✅ Create semantic views
- ✅ Generate Snowsight instructions

## 📊 Expected Output

### Successful Run Output:
```
🚀 Snowflake Semantic Model Pipeline
============================================================

📋 STEP 1/7: Validating Prerequisites
--------------------------------------------------
✅ Configuration is valid!

📋 STEP 2/7: Installing Requirements
--------------------------------------------------
✅ All packages installed successfully!

📋 STEP 3/7: Checking Split Files
--------------------------------------------------
✅ Split files check completed!

📋 STEP 4/7: Loading Data into Snowflake
--------------------------------------------------
📊 Data Loading Results:
  - ANON_VIEWS: 2,500,000 rows loaded
  - ANON_USER_DAY_FACT: 722,374 rows loaded
  - ANON_COMPANY_DAY_FACT: 331,093 rows loaded
✅ Total rows loaded: 3,553,467

📋 STEP 5/7: Verifying Data Integrity
--------------------------------------------------
✅ 3/3 tables have perfect integrity

📋 STEP 6/7: Creating Semantic Model Infrastructure
--------------------------------------------------
✅ Semantic model infrastructure created!

📋 STEP 7/7: Generating Final Instructions
--------------------------------------------------
🎯 SNOWSIGHT SETUP INSTRUCTIONS:
============================================================

🎉 PIPELINE COMPLETED SUCCESSFULLY!
============================================================
```

## 🔧 Manual Deployment (Alternative)

If you prefer step-by-step control:

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Test Configuration
```bash
python config.py
```

### 3. Check Files
```bash
python scripts/check_split_files.py
```

### 4. Load Data
```bash
python scripts/load_split_files.py
```

### 5. Verify Data
```bash
python scripts/verify_data_integrity.py
```

### 6. Create Semantic Model
```bash
python scripts/create_semantic_model_from_yaml.py
```

## 🎯 Snowsight Final Setup

After successful pipeline execution:

### 1. Access Snowsight
- URL: `https://your-account.snowflakecomputing.com`
- Login with your credentials

### 2. Create Semantic Model
- Navigate to **Data** > **Semantic Models**
- Click **"Create Semantic Model"**
- Model Name: `PAID_APP_PLATFORM_ANON`

### 3. Add Base Tables
- `SEMANTIC_ANON_VIEWS` (2.5M rows)
- `SEMANTIC_ANON_USER_DAY_FACT` (722K rows)
- `SEMANTIC_ANON_COMPANY_DAY_FACT` (331K rows)

### 4. Configure Relationships
- Connect using `USER_ID`, `COMPANY_ID`, and `VIEW_DATE`

### 5. Test Natural Language
- "Show me total revenue by company status"
- "What is the average view time per user?"
- "How many paying customers viewed apps last month?"

## 🚨 Troubleshooting

### Common Issues & Solutions:

**❌ "Connection failed"**
```
Solution: Check your Snowflake credentials in config.py
- Verify account identifier (without .snowflakecomputing.com)
- Ensure warehouse is running
- Check network connectivity
```

**❌ "Split files not found"**
```
Solution: Verify file locations
- Check SPLIT_FILES_DIRECTORY path in config.py
- Ensure CSV files exist in the directory
- Verify file naming patterns match expected format
```

**❌ "Column mapping errors"**
```
Solution: Update column mappings in config.py
- Check COLUMN_MAPPINGS dictionary
- Verify CSV column names match expectations
- Update mappings for your specific data format
```

**❌ "Permission denied"**
```
Solution: Check Snowflake permissions
- Ensure your role has CREATE DATABASE privileges
- Verify warehouse usage permissions
- Check schema creation rights
```

### Getting Detailed Logs:

The pipeline creates detailed log files:
```bash
# Check the latest log file
ls -la pipeline_*.log

# View the log
cat pipeline_20240809_182345.log
```

## 📝 Customization for Different Data

### For Different CSV Structure:

1. **Update column mappings** in `config.py`:
```python
COLUMN_MAPPINGS = {
    'YOUR_TABLE': {
        'csv_column_name': 'SNOWFLAKE_COLUMN_NAME',
        # Add your mappings here
    }
}
```

2. **Update table mappings**:
```python
TABLE_MAPPINGS = {
    'your_csv_file': 'YOUR_SNOWFLAKE_TABLE',
    # Add your mappings here
}
```

3. **Modify semantic model YAML** with your schema definition

### For Different Snowflake Setup:

1. **Update database/schema names** in `config.py`
2. **Change warehouse settings** as needed
3. **Modify role permissions** if required

## ✅ Success Validation

Your deployment is successful when:

- [ ] All CSV files loaded without errors
- [ ] Row counts match between local files and Snowflake
- [ ] No critical data integrity issues
- [ ] Semantic views created successfully
- [ ] Snowsight instructions generated
- [ ] Natural language queries work in Snowflake

## 📞 Support

If you encounter issues:

1. **Check the generated log files** for detailed error messages
2. **Run individual scripts** to isolate problems
3. **Verify prerequisites** are met
4. **Review configuration** settings
5. **Test Snowflake connectivity** separately

## 🎉 Next Steps

After successful deployment:

1. **Explore your data** using Snowsight
2. **Create dashboards** with your semantic model
3. **Train users** on natural language queries
4. **Set up monitoring** for data freshness
5. **Plan regular data updates** if needed

---

**🚀 You're now ready to deploy the Snowflake semantic model pipeline in any environment!**