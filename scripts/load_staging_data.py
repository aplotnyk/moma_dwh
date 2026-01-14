"""
MoMA Staging Data Loader
Loads CSV files into PostgreSQL staging tables
"""

import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, text
import os
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import sys

# Load environment variables
load_dotenv()

# Add project root to Python path so we can import config
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DB_CONNECTION_STRING, FILES

def create_db_connection():
# Create database connection using SQLAlchemy with connection string from config.py
    return create_engine(DB_CONNECTION_STRING)

def clear_staging_tables(engine):
# Clear existing data from staging tables
    print("\nClearing existing staging data...")
    
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE staging.staging_moma_artworks"))
        conn.execute(text("TRUNCATE TABLE staging.staging_moma_artists"))
        conn.commit()
    
    print("✅ Staging tables cleared")

def handle_duplicates(df, id_column, table_name):
# Detect and remove duplicate IDs in a DataFrame
    print(f"Checking for duplicate {id_column} values in {table_name}...")

    if id_column not in df.columns:
        print(f"!!! ERROR: Column '{id_column}' not found in {table_name}. Skipping duplicate check.")
        return df

    duplicate_mask = df[id_column].duplicated(keep=False)
    if duplicate_mask.any():
        duplicates = df.loc[duplicate_mask, id_column]
        print(f"⚠️  Found {duplicates.nunique()} duplicate {id_column} values in {table_name}.")
        print("Examples of duplicates:")
        print(duplicates.value_counts().head(5))
        df = df.drop_duplicates(subset=[id_column], keep='first')
        print(f"✅ Kept first occurrence of each unique {id_column}.")
    else:
        print(f"✅ No duplicates found for {id_column}.")

    return df

def load_artworks_csv(csv_path, engine):
# Load Artworks CSV into staging table
    print(f"\n{'='*60}")
    print("Loading MoMA Artworks data...")
    print(f"{'='*60}")
    
    # Read CSV
    print(f"Reading CSV from: {csv_path}")
    df = pd.read_csv(csv_path, low_memory=False)
    
    print(f"Rows read: {len(df):,}")
    print(f"Columns: {len(df.columns)}")
    
    # Prepare column mapping (CSV columns to database columns)
    column_mapping = {
        'Title': 'title',
        'Artist': 'artist',
        'ConstituentID': 'constituent_id',
        'ArtistBio': 'artist_bio',
        'Nationality': 'nationality',
        'BeginDate': 'begin_date',
        'EndDate': 'end_date',
        'Gender': 'gender',
        'Date': 'date',
        'Medium': 'medium',
        'Dimensions': 'dimensions',
        'CreditLine': 'credit_line',
        'AccessionNumber': 'accession_number',
        'Classification': 'classification',
        'Department': 'department',
        'DateAcquired': 'date_acquired',
        'Cataloged': 'cataloged',
        'ObjectID': 'object_id',
        'URL': 'url',
        'ImageURL': 'image_url',
        'OnView': 'on_view',
        'Circumference (cm)': 'circumference_cm',
        'Depth (cm)': 'depth_cm',
        'Diameter (cm)': 'diameter_cm',
        'Height (cm)': 'height_cm',
        'Length (cm)': 'length_cm',
        'Weight (kg)': 'weight_kg',
        'Width (cm)': 'width_cm',
        'Seat Height (cm)': 'seat_height_cm',
        'Duration (sec.)': 'duration_sec'
    }
    
    df = df.rename(columns=column_mapping)

    df['source_file'] = Path(csv_path).name
    
    # Convert all columns to string (since staging uses TEXT)
    for col in df.columns:
        df[col] = df[col].astype(str)
    
    # Replace 'nan' strings with None
    df = df.replace('nan', None)

    # Handle duplicate object_ids
    df = handle_duplicates(df, 'object_id', 'staging_moma_artworks')
    
    print("\nLoading data into PostgreSQL...")
    
    # Load to database
    df.to_sql(
        name='staging_moma_artworks',
        schema='staging',
        con=engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    print(f"✅ Successfully loaded {len(df):,} artwork records")
    return len(df)

def load_artists_csv(csv_path, engine):
# Load Artists CSV into staging table
    print(f"\n{'='*60}")
    print("Loading MoMA Artists data...")
    print(f"{'='*60}")
    
    # Read CSV
    print(f"Reading CSV from: {csv_path}")
    df = pd.read_csv(csv_path, low_memory=False)
    
    print(f"Rows read: {len(df):,}")
    print(f"Columns: {len(df.columns)}")
    
    # Column mapping
    column_mapping = {
        'ConstituentID': 'constituent_id',
        'DisplayName': 'display_name',
        'ArtistBio': 'artist_bio',
        'Nationality': 'nationality',
        'Gender': 'gender',
        'BeginDate': 'begin_date',
        'EndDate': 'end_date',
        'Wiki QID': 'wiki_qid',
        'ULAN': 'ulan'
    }
    
    # Rename columns
    df = df.rename(columns=column_mapping)

    # Add metadata
    df['source_file'] = Path(csv_path).name
    
    # Convert to string
    for col in df.columns:
        df[col] = df[col].astype(str)
    
    # Replace 'nan' with None
    df = df.replace('nan', None)

    # Handle duplicate constituent_ids
    df = handle_duplicates(df, 'constituent_id', 'staging_moma_artists')
    
    print("\nLoading data into PostgreSQL...")
    
    # Load to database
    df.to_sql(
        name='staging_moma_artists',
        schema='staging',
        con=engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    print(f"✅ Successfully loaded {len(df):,} artist records")
    return len(df)

def verify_data_load(engine):
# Verify data was loaded correctly
    print(f"\n{'='*60}")
    print("Verifying data load...")
    print(f"{'='*60}")
    
    queries = {
        'Artworks count': 'SELECT COUNT(*) FROM staging.staging_moma_artworks',
        'Artists count': 'SELECT COUNT(*) FROM staging.staging_moma_artists',
        'Sample artwork titles': 'SELECT title FROM staging.staging_moma_artworks WHERE title IS NOT NULL LIMIT 5',
        'Sample artist names': 'SELECT display_name FROM staging.staging_moma_artists WHERE display_name IS NOT NULL LIMIT 5'
    }
    
    with engine.connect() as conn:
        for description, query in queries.items():
            print(f"\n{description}:")
            result = pd.read_sql(query, conn)
            print(result.to_string(index=False))

# ============================================================================
# AIRFLOW-CALLABLES
# ============================================================================

def load_artworks_to_staging():
    # Airflow-callable function to load artworks
    print("\n" + "="*60)
    print("AIRFLOW TASK: Load Artworks to Staging")
    print("="*60)
    
    try:
        # Get path from config
        csv_path = str(FILES['artworks_csv'])
        
        # Check if file exists
        if not Path(csv_path).exists():
            raise FileNotFoundError(f"Artworks CSV not found at: {csv_path}")
        
        # Create connection
        engine = create_db_connection()
        
        # Clear existing data
        clear_staging_tables(engine)
        
        # Load data
        row_count = load_artworks_csv(csv_path, engine)
        
        print(f"\n✅ Task completed successfully: {row_count:,} rows loaded")
        return row_count
        
    except Exception as e:
        print(f"\n❌ Task failed: {e}")
        raise

def load_artists_to_staging():
    # Airflow-callable function to load artists
    print("\n" + "="*60)
    print("AIRFLOW TASK: Load Artists to Staging")
    print("="*60)
    
    try:
        # Get path from config
        csv_path = str(FILES['artists_csv'])
        
        # Check if file exists
        if not Path(csv_path).exists():
            raise FileNotFoundError(f"Artists CSV not found at: {csv_path}")
        
        # Create connection
        engine = create_db_connection()
        
        # Load data
        row_count = load_artists_csv(csv_path, engine)
        
        print(f"\n✅ Task completed successfully: {row_count:,} rows loaded")
        return row_count
        
    except Exception as e:
        print(f"\n❌ Task failed: {e}")
        raise

def load_all_staging_data():
    # Airflow-callable function to load BOTH artworks and artists in a single task
    print("\n" + "="*60)
    print("AIRFLOW TASK: Load All Staging Data")
    print("="*60)
    
    try:
        artworks_count = load_artworks_to_staging()
        artists_count = load_artists_to_staging()
        
        # Verify
        engine = create_db_connection()
        verify_data_load(engine)
        
        result = {
            'artworks': artworks_count,
            'artists': artists_count,
            'total': artworks_count + artists_count
        }
        
        print(f"\n✅ All staging data loaded successfully!")
        print(f"   Artworks: {artworks_count:,}")
        print(f"   Artists: {artists_count:,}")
        
        return result
        
    except Exception as e:
        print(f"\n❌ Task failed: {e}")
        raise

# ============================================================================
# ORIGINAL MAIN FUNCTION (for standalone/testing use)
# IGNORED by Airflow - only used when running script directly
# ============================================================================

def main():
    print("="*60)
    print("MoMA STAGING DATA LOADER (Manual Mode)")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # For manual testing, prompt for paths
    print("Please provide the paths to your CSV files:")
    artworks_path = input("Path to Artworks.csv: ").strip().strip('"')
    artists_path = input("Path to Artists.csv: ").strip().strip('"')
    
    # Verify files exist
    if not Path(artworks_path).exists():
        print(f"ERROR: Cannot find {artworks_path}")
        return
    if not Path(artists_path).exists():
        print(f"ERROR: Cannot find {artists_path}")
        return
    
    try:
        # Create database connection
        print("\nConnecting to PostgreSQL...")
        engine = create_db_connection()
        print("✅ Connected successfully")

        # Clear existing data
        clear_staging_tables(engine)
        
        # Load artworks
        artworks_count = load_artworks_csv(artworks_path, engine)
        
        # Load artists
        artists_count = load_artists_csv(artists_path, engine)
        
        # Verify
        verify_data_load(engine)
        
        # Summary
        print(f"\n{'='*60}")
        print("LOAD COMPLETE!")
        print(f"{'='*60}")
        print(f"Total artworks loaded: {artworks_count:,}")
        print(f"Total artists loaded: {artists_count:,}")
        print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("\nYou can now query the data in DBeaver!")
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()

# ============================================================================
# SCRIPT ENTRY POINT
# ============================================================================
# This condition is TRUE only when running: python load_staging_data.py
# This condition is FALSE when Airflow imports this module
if __name__ == "__main__":
    main()