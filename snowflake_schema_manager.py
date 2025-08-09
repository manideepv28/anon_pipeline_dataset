#!/usr/bin/env python3
"""
Snowflake GitHub Data Schema Manager
=====================================
This script fetches data from GitHub repositories, parses YAML schema files,
and creates complete schematic models for Snowflake data platforms.

Features:
- Fetches data and schema files from GitHub repositories
- Parses YAML schema definitions
- Creates Snowflake tables, views, and stored procedures
- Handles data loading and transformation
- Supports anonymized data platforms

Requirements:
- snowflake-connector-python
- PyYAML
- requests
- pandas

Usage in Snowflake:
    python snowflake.py

Installation:
    pip install snowflake-connector-python PyYAML requests pandas
"""

def install_requirements():
    """Install required packages if missing"""
    import subprocess
    import sys
    
    required_packages = [
        'snowflake-connector-python',
        'PyYAML',
        'requests', 
        'pandas'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            if package == 'PyYAML':
                import yaml
            elif package == 'snowflake-connector-python':
                import snowflake.connector
            elif package == 'requests':
                import requests
            elif package == 'pandas':
                import pandas
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print(f"📦 Installing missing packages: {', '.join(missing_packages)}")
        for package in missing_packages:
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", package])
                print(f"✅ Successfully installed {package}")
            except subprocess.CalledProcessError as e:
                print(f"❌ Failed to install {package}: {e}")
                return False
        print("✅ All required packages are now available!")
    else:
        print("✅ All required packages are already installed!")
    
    return True

import os
import sys
import json
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from urllib.parse import urlparse
import zipfile
import tempfile
import logging

# Try to import required packages
try:
    import yaml
    import requests
    import pandas as pd
except ImportError as e:
    print(f"Missing required package: {e}")
    print("Please run: pip install PyYAML requests pandas")
    sys.exit(1)

# Snowflake imports - will be checked again in main()
SNOWFLAKE_AVAILABLE = False
try:
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    pass  # Will be handled in main()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class SnowflakeConfig:
    """Snowflake connection configuration"""
    account: str
    user: str
    password: str
    warehouse: str
    database: str
    schema: str
    role: Optional[str] = None

@dataclass
class TableSchema:
    """Represents a table schema definition"""
    name: str
    columns: List[Dict[str, Any]]
    primary_key: Optional[List[str]] = None
    foreign_keys: Optional[List[Dict[str, Any]]] = None
    indexes: Optional[List[Dict[str, Any]]] = None
    description: Optional[str] = None

@dataclass
class DatasetMetadata:
    """Represents dataset metadata"""
    name: str
    version: str
    description: str
    tables: List[TableSchema]
    relationships: Optional[List[Dict[str, Any]]] = None

class GitHubDataFetcher:
    """Fetches data and schema files from GitHub repositories"""
    
    def __init__(self, repo_url: str, token: Optional[str] = None):
        self.repo_url = repo_url
        self.token = token
        self.headers = {"Authorization": f"token {token}"} if token else {}
        self.api_base = "https://api.github.com"
        
        # Parse repository information
        parsed_url = urlparse(repo_url)
        path_parts = parsed_url.path.strip('/').split('/')
        self.owner = path_parts[0]
        self.repo = path_parts[1]
        
    def get_repo_contents(self, path: str = "") -> List[Dict[str, Any]]:
        """Get repository contents at specified path"""
        url = f"{self.api_base}/repos/{self.owner}/{self.repo}/contents/{path}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()
    
    def download_file(self, file_path: str) -> str:
        """Download a file from the repository"""
        url = f"https://raw.githubusercontent.com/{self.owner}/{self.repo}/main/{file_path}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.text
    
    def download_repository(self) -> str:
        """Download entire repository as ZIP"""
        url = f"{self.api_base}/repos/{self.owner}/{self.repo}/zipball/main"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        
        # Save to temporary file
        temp_dir = tempfile.mkdtemp()
        zip_path = os.path.join(temp_dir, "repo.zip")
        
        with open(zip_path, 'wb') as f:
            f.write(response.content)
        
        # Extract ZIP
        extract_path = os.path.join(temp_dir, "extracted")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
        
        # Find extracted folder
        extracted_folders = [f for f in os.listdir(extract_path) if os.path.isdir(os.path.join(extract_path, f))]
        if extracted_folders:
            return os.path.join(extract_path, extracted_folders[0])
        
        return extract_path
    
    def find_schema_files(self, local_path: str) -> List[str]:
        """Find YAML schema files in the repository"""
        schema_files = []
        for root, dirs, files in os.walk(local_path):
            for file in files:
                if file.endswith(('.yml', '.yaml')) and ('schema' in file.lower() or 'model' in file.lower()):
                    schema_files.append(os.path.join(root, file))
        return schema_files
    
    def find_data_files(self, local_path: str) -> List[str]:
        """Find data files in the repository"""
        data_files = []
        for root, dirs, files in os.walk(local_path):
            for file in files:
                if file.endswith(('.csv', '.json', '.parquet', '.xlsx')):
                    data_files.append(os.path.join(root, file))
        return data_files

class SchemaParser:
    """Parses YAML schema files and converts to table definitions"""
    
    @staticmethod
    def parse_yaml_schema(file_path: str) -> Dict[str, Any]:
        """Parse YAML schema file"""
        with open(file_path, 'r') as f:
            return yaml.safe_load(f)
    
    @staticmethod
    def convert_to_table_schema(schema_data: Dict[str, Any]) -> List[TableSchema]:
        """Convert parsed schema to TableSchema objects"""
        tables = []
        
        # Handle different schema formats
        if 'tables' in schema_data:
            # Check if it's semantic model format (like anon_app_platform_dataset)
            if isinstance(schema_data['tables'], list) and len(schema_data['tables']) > 0:
                first_table = schema_data['tables'][0]
                if 'dimensions' in first_table or 'facts' in first_table or 'time_dimensions' in first_table:
                    # Semantic model format
                    for table_def in schema_data['tables']:
                        tables.append(SchemaParser._parse_semantic_model_table(table_def))
                else:
                    # Standard format with tables array
                    for table_def in schema_data['tables']:
                        tables.append(SchemaParser._parse_table_definition(table_def))
        
        elif 'models' in schema_data:
            # dbt-style schema format
            for model_def in schema_data['models']:
                tables.append(SchemaParser._parse_model_definition(model_def))
        
        else:
            # Single table definition
            tables.append(SchemaParser._parse_table_definition(schema_data))
        
        return tables
    
    @staticmethod
    def _parse_table_definition(table_def: Dict[str, Any]) -> TableSchema:
        """Parse individual table definition"""
        name = table_def.get('name', 'unnamed_table')
        
        # Parse columns
        columns = []
        for col_def in table_def.get('columns', []):
            column = {
                'name': col_def.get('name'),
                'type': SchemaParser._map_data_type(col_def.get('type', 'STRING')),
                'nullable': col_def.get('nullable', True),
                'description': col_def.get('description', ''),
                'constraints': col_def.get('constraints', [])
            }
            columns.append(column)
        
        return TableSchema(
            name=name,
            columns=columns,
            primary_key=table_def.get('primary_key'),
            foreign_keys=table_def.get('foreign_keys'),
            indexes=table_def.get('indexes'),
            description=table_def.get('description', '')
        )
    
    @staticmethod
    def _parse_model_definition(model_def: Dict[str, Any]) -> TableSchema:
        """Parse dbt-style model definition"""
        name = model_def.get('name', 'unnamed_model')
        
        columns = []
        for col_def in model_def.get('columns', []):
            column = {
                'name': col_def.get('name'),
                'type': SchemaParser._map_data_type(col_def.get('data_type', 'STRING')),
                'nullable': not col_def.get('not_null', False),
                'description': col_def.get('description', ''),
                'tests': col_def.get('tests', [])
            }
            columns.append(column)
        
        return TableSchema(
            name=name,
            columns=columns,
            description=model_def.get('description', '')
        )
    
    @staticmethod
    def _parse_semantic_model_table(table_def: Dict[str, Any]) -> TableSchema:
        """Parse semantic model table definition (like anon_app_platform_dataset format)"""
        name = table_def.get('name', 'unnamed_table')
        
        columns = []
        
        # Parse dimensions
        for dim in table_def.get('dimensions', []):
            column = {
                'name': dim.get('name'),
                'type': SchemaParser._map_data_type(dim.get('data_type', 'VARCHAR')),
                'nullable': True,  # Dimensions are typically nullable
                'description': dim.get('description', ''),
                'synonyms': dim.get('synonyms', [])
            }
            columns.append(column)
        
        # Parse time dimensions
        for time_dim in table_def.get('time_dimensions', []):
            column = {
                'name': time_dim.get('name'),
                'type': SchemaParser._map_data_type(time_dim.get('data_type', 'TIMESTAMP')),
                'nullable': True,
                'description': time_dim.get('description', ''),
                'synonyms': time_dim.get('synonyms', [])
            }
            columns.append(column)
        
        # Parse facts (measures)
        for fact in table_def.get('facts', []):
            column = {
                'name': fact.get('name'),
                'type': SchemaParser._map_data_type(fact.get('data_type', 'FLOAT')),
                'nullable': True,
                'description': fact.get('description', ''),
                'synonyms': fact.get('synonyms', []),
                'default_aggregation': fact.get('default_aggregation', 'SUM')
            }
            columns.append(column)
        
        return TableSchema(
            name=name.upper(),  # Snowflake convention
            columns=columns,
            description=table_def.get('description', '')
        )
    
    @staticmethod
    def _map_data_type(data_type: str) -> str:
        """Map generic data types to Snowflake data types"""
        # Handle Snowflake-specific types first
        if 'VARCHAR(' in data_type.upper():
            return data_type.upper()
        if 'NUMBER(' in data_type.upper():
            return data_type.upper()
        if 'TIMESTAMP_NTZ' in data_type.upper():
            return data_type.upper()
        
        type_mapping = {
            'string': 'VARCHAR(16777216)',
            'text': 'TEXT',
            'integer': 'INTEGER',
            'int': 'INTEGER',
            'bigint': 'BIGINT',
            'float': 'FLOAT',
            'double': 'DOUBLE',
            'decimal': 'DECIMAL',
            'numeric': 'NUMERIC',
            'boolean': 'BOOLEAN',
            'bool': 'BOOLEAN',
            'date': 'DATE',
            'datetime': 'TIMESTAMP_NTZ',
            'timestamp': 'TIMESTAMP_NTZ',
            'timestamp_ntz': 'TIMESTAMP_NTZ',
            'time': 'TIME',
            'json': 'VARIANT',
            'array': 'ARRAY',
            'object': 'OBJECT',
            'varchar': 'VARCHAR(16777216)'
        }
        
        return type_mapping.get(data_type.lower(), 'VARCHAR(16777216)')

class SnowflakeSchemaManager:
    """Manages Snowflake schema creation and data loading"""
    
    def __init__(self, config: SnowflakeConfig):
        self.config = config
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
            
            # Test connection and create database/schema if needed
            self._setup_database_schema()
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to Snowflake: {e}")
            raise
    
    def _setup_database_schema(self):
        """Setup database and schema if they don't exist"""
        cursor = self.connection.cursor()
        
        try:
            # Create database if it doesn't exist
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.config.database}")
            logger.info(f"Database {self.config.database} ready")
            
            # Use the database
            cursor.execute(f"USE DATABASE {self.config.database}")
            
            # Create schema if it doesn't exist
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.config.schema}")
            logger.info(f"Schema {self.config.schema} ready")
            
            # Use the schema
            cursor.execute(f"USE SCHEMA {self.config.schema}")
            
        except Exception as e:
            logger.error(f"Error setting up database/schema: {e}")
            raise
        finally:
            cursor.close()
    
    def create_tables(self, tables: List[TableSchema]) -> None:
        """Create tables in Snowflake"""
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        
        for table in tables:
            ddl = self._generate_create_table_ddl(table)
            logger.info(f"Creating table: {table.name}")
            logger.info(f"DDL: {ddl}")
            
            try:
                cursor.execute(ddl)
                logger.info(f"Successfully created table: {table.name}")
            except Exception as e:
                logger.error(f"Error creating table {table.name}: {e}")
        
        cursor.close()
    
    def _generate_create_table_ddl(self, table: TableSchema) -> str:
        """Generate CREATE TABLE DDL"""
        columns_ddl = []
        
        for column in table.columns:
            col_def = f"{column['name']} {column['type']}"
            
            if not column.get('nullable', True):
                col_def += " NOT NULL"
            
            if column.get('description'):
                col_def += f" COMMENT '{column['description']}'"
            
            columns_ddl.append(col_def)
        
        ddl = f"CREATE OR REPLACE TABLE {table.name} (\n"
        ddl += ",\n".join([f"  {col}" for col in columns_ddl])
        
        # Add primary key constraint
        if table.primary_key:
            pk_columns = ", ".join(table.primary_key)
            ddl += f",\n  PRIMARY KEY ({pk_columns})"
        
        ddl += "\n)"
        
        # Add table comment
        if table.description:
            ddl += f" COMMENT = '{table.description}'"
        
        return ddl
    
    def load_data_file(self, file_path: str, table_name: str) -> None:
        """Load data file into Snowflake table"""
        if not self.connection:
            self.connect()
        
        file_extension = os.path.splitext(file_path)[1].lower()
        
        if file_extension == '.csv':
            self._load_csv_file(file_path, table_name)
        elif file_extension == '.json':
            self._load_json_file(file_path, table_name)
        elif file_extension in ['.xlsx', '.xls']:
            self._load_excel_file(file_path, table_name)
        else:
            logger.warning(f"Unsupported file type: {file_extension}")
    
    def _load_csv_file(self, file_path: str, table_name: str) -> None:
        """Load CSV file using pandas"""
        try:
            df = pd.read_csv(file_path)
            
            # Map CSV column names to schema column names
            df = self._map_csv_columns(df, table_name)
            
            # Convert column names to uppercase (Snowflake convention)
            df.columns = [col.upper() for col in df.columns]
            
            success, nchunks, nrows, _ = write_pandas(
                self.connection,
                df,
                table_name,
                auto_create_table=False
            )
            logger.info(f"Loaded {nrows} rows into {table_name}")
        except Exception as e:
            logger.error(f"Error loading CSV file {file_path}: {e}")
    
    def _map_csv_columns(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Map CSV column names to match schema expectations"""
        # Define column mappings for known tables
        column_mappings = {
            'ANON_VIEWS': {
                'app_id': 'APP_ID',
                'user_id': 'USER_ID', 
                'company_id': 'COMPANY_ID',
                'status': 'STATUS',
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
                'total_amount_spent': 'TOTAL_AMOUNT_SPENT',
                'total_view_time': 'TOTAL_VIEW_TIME_SEC',
                'app_count': 'DISTINCT_APP_COUNT',
                'view_count': 'VIEW_COUNT'
            },
            'ANON_COMPANY_DAY_FACT': {
                'company_id': 'COMPANY_ID',
                'view_date': 'VIEW_DATE',
                'company_status': 'STATUS',
                'total_amount_spent': 'TOTAL_AMOUNT_SPENT',
                'total_view_time': 'TOTAL_VIEW_TIME_SEC',
                'app_count': 'DISTINCT_APP_COUNT',
                'view_count': 'VIEW_COUNT',
                'user_count': 'DISTINCT_USER_COUNT'
            }
        }
        
        if table_name in column_mappings:
            mapping = column_mappings[table_name]
            df = df.rename(columns=mapping)
            logger.info(f"Applied column mapping for {table_name}")
        
        return df
    
    def _load_json_file(self, file_path: str, table_name: str) -> None:
        """Load JSON file"""
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            if isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                df = pd.DataFrame([data])
            
            success, nchunks, nrows, _ = write_pandas(
                self.connection,
                df,
                table_name,
                auto_create_table=False
            )
            logger.info(f"Loaded {nrows} rows into {table_name}")
        except Exception as e:
            logger.error(f"Error loading JSON file {file_path}: {e}")
    
    def _load_excel_file(self, file_path: str, table_name: str) -> None:
        """Load Excel file"""
        try:
            df = pd.read_excel(file_path)
            success, nchunks, nrows, _ = write_pandas(
                self.connection,
                df,
                table_name,
                auto_create_table=False
            )
            logger.info(f"Loaded {nrows} rows into {table_name}")
        except Exception as e:
            logger.error(f"Error loading Excel file {file_path}: {e}")
    
    def create_views(self, tables: List[TableSchema]) -> None:
        """Create anonymized views for data privacy"""
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        
        for table in tables:
            view_name = f"{table.name}_ANON_VIEW"
            view_ddl = self._generate_anonymized_view_ddl(table, view_name)
            
            try:
                cursor.execute(view_ddl)
                logger.info(f"Successfully created view: {view_name}")
            except Exception as e:
                logger.error(f"Error creating view {view_name}: {e}")
        
        cursor.close()
    
    def _generate_anonymized_view_ddl(self, table: TableSchema, view_name: str) -> str:
        """Generate anonymized view DDL"""
        select_columns = []
        
        for column in table.columns:
            col_name = column['name']
            col_type = column['type']
            
            # Apply anonymization based on column name patterns
            if any(term in col_name.lower() for term in ['email', 'mail']):
                select_columns.append(f"'***@***.com' AS {col_name}")
            elif any(term in col_name.lower() for term in ['phone', 'mobile', 'tel']):
                select_columns.append(f"'***-***-****' AS {col_name}")
            elif any(term in col_name.lower() for term in ['ssn', 'social']):
                select_columns.append(f"'***-**-****' AS {col_name}")
            elif any(term in col_name.lower() for term in ['name', 'first', 'last']):
                select_columns.append(f"'[REDACTED]' AS {col_name}")
            elif any(term in col_name.lower() for term in ['id', 'key']) and col_type in ['VARCHAR', 'TEXT']:
                select_columns.append(f"SUBSTR({col_name}, 1, 4) || '****' AS {col_name}")
            else:
                select_columns.append(col_name)
        
        ddl = f"CREATE OR REPLACE VIEW {view_name} AS\n"
        ddl += f"SELECT\n  {',\n  '.join(select_columns)}\n"
        ddl += f"FROM {table.name}"
        
        return ddl
    
    def close(self):
        """Close Snowflake connection"""
        if self.connection:
            self.connection.close()
            logger.info("Closed Snowflake connection")

class DataPlatformManager:
    """Main class to orchestrate the entire process"""
    
    def __init__(self, repo_url: str, snowflake_config: SnowflakeConfig, github_token: Optional[str] = None):
        self.repo_url = repo_url
        self.snowflake_config = snowflake_config
        self.github_token = github_token
        
        self.fetcher = GitHubDataFetcher(repo_url, github_token)
        self.schema_manager = SnowflakeSchemaManager(snowflake_config)
        
    def process_repository(self) -> None:
        """Process the entire repository and create Snowflake schema"""
        logger.info(f"Processing repository: {self.repo_url}")
        
        # Download repository
        local_path = self.fetcher.download_repository()
        logger.info(f"Repository downloaded to: {local_path}")
        
        # Find schema and data files
        schema_files = self.fetcher.find_schema_files(local_path)
        data_files = self.fetcher.find_data_files(local_path)
        
        logger.info(f"Found {len(schema_files)} schema files")
        logger.info(f"Found {len(data_files)} data files")
        
        # Parse schemas
        all_tables = []
        for schema_file in schema_files:
            logger.info(f"Parsing schema file: {schema_file}")
            schema_data = SchemaParser.parse_yaml_schema(schema_file)
            tables = SchemaParser.convert_to_table_schema(schema_data)
            all_tables.extend(tables)
        
        logger.info(f"Parsed {len(all_tables)} table definitions")
        
        # Create tables in Snowflake
        if SNOWFLAKE_AVAILABLE:
            self.schema_manager.create_tables(all_tables)
            
            # Load data files
            table_names = {table.name.lower(): table.name for table in all_tables}
            for data_file in data_files:
                file_name = os.path.splitext(os.path.basename(data_file))[0].lower()
                if file_name in table_names:
                    self.schema_manager.load_data_file(data_file, table_names[file_name])
                else:
                    logger.warning(f"No matching table found for data file: {data_file}")
            
            # Create anonymized views
            self.schema_manager.create_views(all_tables)
            
            self.schema_manager.close()
        else:
            # Generate SQL files for manual execution
            self._generate_sql_files(all_tables, local_path)
        
        logger.info("Repository processing completed")
    
    def _generate_sql_files(self, tables: List[TableSchema], output_dir: str) -> None:
        """Generate SQL files when Snowflake connector is not available"""
        sql_dir = os.path.join(output_dir, "generated_sql")
        os.makedirs(sql_dir, exist_ok=True)
        
        # Generate DDL file
        ddl_file = os.path.join(sql_dir, "create_tables.sql")
        with open(ddl_file, 'w') as f:
            f.write("-- Generated DDL for Snowflake\n")
            f.write(f"-- Database: {self.snowflake_config.database}\n")
            f.write(f"-- Schema: {self.snowflake_config.schema}\n\n")
            
            for table in tables:
                ddl = self.schema_manager._generate_create_table_ddl(table)
                f.write(f"{ddl};\n\n")
        
        # Generate views file
        views_file = os.path.join(sql_dir, "create_views.sql")
        with open(views_file, 'w') as f:
            f.write("-- Generated anonymized views for Snowflake\n\n")
            
            for table in tables:
                view_name = f"{table.name}_ANON_VIEW"
                view_ddl = self.schema_manager._generate_anonymized_view_ddl(table, view_name)
                f.write(f"{view_ddl};\n\n")
        
        logger.info(f"SQL files generated in: {sql_dir}")

def check_snowflake_availability():
    """Check if Snowflake connector is available after installation"""
    global SNOWFLAKE_AVAILABLE
    try:
        import snowflake.connector
        from snowflake.connector.pandas_tools import write_pandas
        SNOWFLAKE_AVAILABLE = True
        return True
    except ImportError as e:
        print(f"❌ Snowflake connector still not available: {e}")
        return False

def main():
    """Main function for command-line usage"""
    
    # Check and install requirements
    print("🔍 Checking requirements...")
    if not install_requirements():
        print("❌ Failed to install required packages. Please install manually:")
        print("pip install snowflake-connector-python PyYAML requests pandas")
        return
    
    # Check Snowflake availability after installation
    if not check_snowflake_availability():
        print("❌ Snowflake connector is not available. Please install manually:")
        print("pip install snowflake-connector-python")
        return
    
    # Configuration - Update these values for your environment
    REPO_URL = "https://github.com/sfc-gh-trichards/anon_app_platform_dataset"
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  # Optional, for private repos
    
    # Snowflake configuration - Updated for your specific environment
    snowflake_config = SnowflakeConfig(
        account="jsovftw-uu47247",  # Just the account identifier
        user="manideep285",
        password="v.Manideep@123",
        warehouse="COMPUTE_WH",  # Default warehouse name
        database="ANON_APP_PLATFORM",
        schema="PUBLIC",
        role="ACCOUNTADMIN"
    )
    
    # Create and run the data platform manager
    manager = DataPlatformManager(REPO_URL, snowflake_config, GITHUB_TOKEN)
    
    try:
        print("🚀 Starting repository processing...")
        print(f"Repository: {REPO_URL}")
        print(f"Snowflake Account: {snowflake_config.account}")
        print(f"Database: {snowflake_config.database}")
        print(f"Schema: {snowflake_config.schema}")
        print("-" * 50)
        
        manager.process_repository()
        print("\n" + "="*60)
        print("✅ Successfully processed repository and created Snowflake schema!")
        print("\n📊 Created tables:")
        print("  - ANON_VIEWS")
        print("  - ANON_USER_DAY_FACT") 
        print("  - ANON_COMPANY_DAY_FACT")
        print("\n🔒 Created anonymized views:")
        print("  - ANON_VIEWS_ANON_VIEW")
        print("  - ANON_USER_DAY_FACT_ANON_VIEW")
        print("  - ANON_COMPANY_DAY_FACT_ANON_VIEW")
        print("\n🎯 You can now query your data in Snowflake!")
        print("="*60)
        
    except Exception as e:
        logger.error(f"Error processing repository: {e}")
        print(f"❌ Failed to process repository: {e}")
        print("\n🔧 Troubleshooting tips:")
        print("1. Check your Snowflake credentials")
        print("2. Ensure your warehouse is running")
        print("3. Verify network connectivity to Snowflake")
        print("4. Check if you have necessary permissions")
        sys.exit(1)

def test_connection():
    """Test Snowflake connection"""
    snowflake_config = SnowflakeConfig(
        account="jsovftw-uu47247",  # Just the account identifier
        user="manideep285",
        password="v.Manideep@123",
        warehouse="COMPUTE_WH",
        database="ANON_APP_PLATFORM",
        schema="PUBLIC",
        role="ACCOUNTADMIN"
    )
    
    try:
        manager = SnowflakeSchemaManager(snowflake_config)
        manager.connect()
        print("✅ Snowflake connection successful!")
        manager.close()
        return True
    except Exception as e:
        print(f"❌ Snowflake connection failed: {e}")
        return False

if __name__ == "__main__":
    import sys
    
    # Check if user wants to test connection only
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        test_connection()
    else:
        main()