"""
MoMA Data Warehouse Configuration
Centralized configuration for database connections and paths
"""
import os
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

# Project root directory
PROJECT_ROOT = Path(__file__).parent.absolute()

# Airflow home (where DAGs are stored)
AIRFLOW_HOME = Path.home() / 'Documents' / 'git_moma_dwh'

# Database Configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'moma_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}

# Database connection string
DB_CONNECTION_STRING = (
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# API Configuration
FRED_API_KEY = os.getenv("FRED_API_KEY")
WIKIDATA_USER_AGENT = os.getenv(
    "WIKIDATA_USER_AGENT",
    "MoMA-Data-Project (Educational Project)"
)

# Paths
PATHS = {
    'dags': AIRFLOW_HOME / 'dags',
    'sql': PROJECT_ROOT / 'sql',
    'scripts': PROJECT_ROOT / 'scripts',
    'data': PROJECT_ROOT / 'data',
    'logs': PROJECT_ROOT / 'logs'
}

# Data URLs
MOMA_ARTWORKS_URL = "https://media.githubusercontent.com/media/MuseumofModernArt/collection/refs/heads/main/Artworks.csv"
MOMA_ARTISTS_URL = "https://media.githubusercontent.com/media/MuseumofModernArt/collection/refs/heads/main/Artists.csv"

# File paths
FILES = {
    'artworks_csv': PATHS['data'] / 'Artworks.csv',
    'artists_csv': PATHS['data'] / 'Artists.csv',
    'staging_schema_sql': PATHS['sql'] / 'staging_ddl.sql',
    'transformed_schema_sql': PATHS['sql'] / 'transformed_ddl.sql',
    'dimensional_schema_sql': PATHS['sql'] / 'dimensional_ddl.sql'
}

# Print config on import (helpful for debugging)
if __name__ == "__main__":
    print("=" * 60)
    print("MoMA Project Configuration")
    print("=" * 60)
    print(f"\n📁 Paths:")
    print(f"   Project Root: {PROJECT_ROOT}")
    print(f"   Airflow Home: {AIRFLOW_HOME}")
    print(f"   Scripts: {PATHS['scripts']}")
    print(f"   SQL: {PATHS['sql']}")
    print(f"   Data: {PATHS['data']}")
    print(f"   Logs: {PATHS['logs']}")
    
    print(f"\n💾 Database:")
    print(f"   Host: {DB_CONFIG['host']}")
    print(f"   Port: {DB_CONFIG['port']}")
    print(f"   Database: {DB_CONFIG['database']}")
    print(f"   User: {DB_CONFIG['user']}")
    print(f"   Password: {'*' * len(DB_CONFIG['password'])}")
    
    print(f"\n📂 Folder Existence Check:")
    for name, path in PATHS.items():
        exists = "✅" if path.exists() else "❌ (will be created)"
        print(f"   {name}: {exists}")
    
    print(f"\n📄 Expected Files:")
    for name, path in FILES.items():
        exists = "✅" if path.exists() else "❌ (not created yet)"
        print(f"   {name}: {exists}")