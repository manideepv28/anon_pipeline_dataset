# Snowflake Semantic Model Pipeline

This repository contains a complete pipeline for creating Snowflake semantic models from GitHub data with split CSV files. The pipeline fetches data, loads it into Snowflake, and creates semantic models for business intelligence.

## 🎯 What This Pipeline Does

1. **Fetches data** from GitHub repository with semantic model YAML
2. **Loads split CSV files** into Snowflake tables without data loss
3. **Creates semantic views** optimized for modeling
4. **Generates semantic model** infrastructure in Snowflake
5. **Provides step-by-step guidance** for Snowsight UI setup

## 📁 Required Files Structure

```
your-project/
├── README.md                           # This documentation
├── config.py                          # Configuration file (EDIT THIS)
├── setup_pipeline.py                  # One-command setup script
├── requirements.txt                   # Python dependencies
├── split_files/                       # Your split CSV files directory
│   ├── anon_views_part_01.csv
│   ├── anon_views_part_02.csv
│   ├── anon_user_day_fact.csv
│   └── anon_company_day_fact.csv
├── semantic_model.yaml                # Semantic model definition
└── scripts/                          # Pipeline scripts
    ├── load_split_files.py
    ├── verify_data_integrity.py
    ├── create_semantic_model_from_yaml.py
    └── check_split_files.py
```

## 🚀 Quick Start (One Command Setup)

### Step 1: Edit Configuration
Edit `config.py` with your Snowflake credentials:

```python
# Snowflake Configuration - EDIT THESE VALUES
SNOWFLAKE_ACCOUNT = "your-account-id"           # e.g., "abc12345"
SNOWFLAKE_USER = "your-username"                # e.g., "john_doe"
SNOWFLAKE_PASSWORD = "your-password"            # e.g., "MyPassword123"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"              # Your warehouse name
SNOWFLAKE_DATABASE = "YOUR_DATABASE"            # e.g., "ANALYTICS_DB"
SNOWFLAKE_SCHEMA = "PUBLIC"                     # Schema name
SNOWFLAKE_ROLE = "ACCOUNTADMIN"                 # Your role

# Data Configuration - EDIT IF NEEDED
SPLIT_FILES_DIRECTORY = "split_files"           # Directory with your CSV files
SEMANTIC_MODEL_YAML = "semantic_model.yaml"    # Path to semantic model YAML
```

### Step 2: Run Setup
```bash
python setup_pipeline.py
```

That's it! The script will:
- ✅ Install required packages
- ✅ Connect to Snowflake
- ✅ Create database and schema
- ✅ Load all split files
- ✅ Create semantic views
- ✅ Verify data integrity
- ✅ Generate Snowsight instructions

## 📋 Prerequisites

- Python 3.7+
- Snowflake account with appropriate permissions
- Split CSV files in the specified directory
- Semantic model YAML file

## 🔧 Manual Setup (If Needed)

If you prefer to run steps individually:

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Check Your Files
```bash
python scripts/check_split_files.py
```

### 3. Load Data
```bash
python scripts/load_split_files.py
```

### 4. Verify Data
```bash
python scripts/verify_data_integrity.py
```

### 5. Create Semantic Model
```bash
python scripts/create_semantic_model_from_yaml.py
```

## 📊 Expected Results

After successful execution:

### Data Loaded:
- **ANON_VIEWS**: ~2.5M rows (Individual app view events)
- **ANON_USER_DAY_FACT**: ~722K rows (Daily user metrics)
- **ANON_COMPANY_DAY_FACT**: ~331K rows (Daily company metrics)

### Semantic Views Created:
- **SEMANTIC_ANON_VIEWS**
- **SEMANTIC_ANON_USER_DAY_FACT**
- **SEMANTIC_ANON_COMPANY_DAY_FACT**

### Ready for Snowsight:
- Complete semantic model structure
- Configured dimensions and measures
- Natural language query support

## 🎯 Snowsight Setup

After running the pipeline, follow the generated instructions to:

1. **Access Snowsight**: `https://your-account.snowflakecomputing.com`
2. **Navigate to**: Data > Semantic Models
3. **Create Model**: Use the generated views as base tables
4. **Configure**: Dimensions, measures, and relationships
5. **Test**: Natural language queries

## 🔍 Troubleshooting

### Common Issues:

**Connection Failed:**
- Check your Snowflake credentials in `config.py`
- Verify your account identifier format
- Ensure your warehouse is running

**File Not Found:**
- Verify split files are in the correct directory
- Check file names match expected patterns
- Ensure semantic_model.yaml exists

**Data Loading Errors:**
- Check column mappings in load_split_files.py
- Verify CSV file formats
- Review Snowflake permissions

### Getting Help:

1. Check the generated logs for detailed error messages
2. Run verification scripts to identify issues
3. Review the troubleshooting section in individual scripts

## 📝 Customization

### For Different Data Sources:

1. **Update column mappings** in `load_split_files.py`
2. **Modify semantic model YAML** for your schema
3. **Adjust file patterns** in `check_split_files.py`
4. **Update configuration** in `config.py`

### For Different Snowflake Setup:

1. **Change database/schema names** in `config.py`
2. **Update warehouse settings** as needed
3. **Modify role permissions** if required

## 🎉 Success Criteria

Your pipeline is successful when:

- ✅ All CSV files loaded without data loss
- ✅ Row counts match between local files and Snowflake
- ✅ Semantic views created successfully
- ✅ No duplicate records (or acceptable duplicates)
- ✅ Snowsight instructions generated
- ✅ Natural language queries work in Snowflake

## 📚 Additional Resources

- [Snowflake Semantic Models Documentation](https://docs.snowflake.com/en/user-guide/semantic-models)
- [Snowsight User Guide](https://docs.snowflake.com/en/user-guide/ui-snowsight)
- [Python Connector Documentation](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector)

## 🤝 Contributing

To improve this pipeline:

1. Fork the repository
2. Make your changes
3. Test with your own data
4. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.