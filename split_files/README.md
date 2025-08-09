# Split Files Directory

This directory should contain your split CSV files for the semantic model pipeline.

## Expected Files

Place your split CSV files here:

- `anon_views_part_01.csv` - First part of anonymized view events
- `anon_views_part_02.csv` - Second part of anonymized view events  
- `anon_user_day_fact.csv` - Daily user-level aggregations
- `anon_company_day_fact.csv` - Daily company-level aggregations

## File Format

Each CSV file should have:
- Header row with column names
- Proper data types (dates, numbers, strings)
- Consistent formatting across split files

## Data Size

Expected data volumes:
- **ANON_VIEWS**: ~2.5M rows (split across 2 files)
- **ANON_USER_DAY_FACT**: ~722K rows (1 file)
- **ANON_COMPANY_DAY_FACT**: ~331K rows (1 file)

## Usage

The pipeline will automatically:
1. Detect all CSV files in this directory
2. Map them to appropriate Snowflake tables
3. Load data with proper column mapping
4. Verify data integrity

## Note

Due to file size limitations, the actual CSV files are not included in this repository. 
You'll need to obtain the data files separately and place them in this directory before running the pipeline.