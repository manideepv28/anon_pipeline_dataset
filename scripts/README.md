# Scripts Directory

This directory contains the individual pipeline scripts. These are called automatically by `setup_pipeline.py`, but can also be run individually for debugging or customization.

## 📁 Script Files

Copy these files to the `scripts/` directory:

- `load_split_files.py` - Loads split CSV files into Snowflake
- `verify_data_integrity.py` - Verifies data integrity after loading
- `create_semantic_model_from_yaml.py` - Creates semantic model from YAML
- `check_split_files.py` - Checks and validates split files

## 🔧 Individual Usage

Each script can be run independently:

```bash
# Check your split files
python scripts/check_split_files.py

# Load data into Snowflake
python scripts/load_split_files.py

# Verify data integrity
python scripts/verify_data_integrity.py

# Create semantic model
python scripts/create_semantic_model_from_yaml.py
```

## ⚙️ Configuration

All scripts read from the main `config.py` file in the root directory. Make sure to configure your credentials there before running any scripts.