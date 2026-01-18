"""
Transform Artists Data - Memory Optimized Version
Cleans, standardizes, and enriches artist data from staging
"""

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import os
from pathlib import Path
import sys
import psycopg2

# Load environment variables
load_dotenv()

# Add project root to Python path so we can import config
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DB_CONNECTION_STRING

def create_db_connection():
    """Create database connection using SQLAlchemy with connection string from config.py"""
    engine = create_engine(DB_CONNECTION_STRING)
    return engine

def create_psycopg2_connection():
    """Create a raw psycopg2 connection for pandas compatibility"""
    from urllib.parse import urlparse
    
    result = urlparse(DB_CONNECTION_STRING)
    
    conn = psycopg2.connect(
        database=result.path[1:],
        user=result.username,
        password=result.password,
        host=result.hostname,
        port=result.port
    )
    return conn

def load_staging_artists(engine):
    """Load artists from staging - optimized to use less memory"""
    print("Loading artists from staging...")
    
    # Only load columns we actually need
    query = """
        SELECT 
            constituent_id,
            display_name,
            nationality,
            gender,
            begin_date,
            end_date,
            wiki_qid
        FROM staging.staging_moma_artists
        WHERE constituent_id IS NOT NULL
    """
    
    conn = create_psycopg2_connection()
    try:
        # Read with dtype optimization to reduce memory usage
        df = pd.read_sql_query(
            query, 
            conn,
            dtype={
                'constituent_id': 'Int32',  # Use nullable integer
                'display_name': 'string',
                'nationality': 'string',
                'gender': 'string',
                'begin_date': 'string',
                'end_date': 'string',
                'wiki_qid': 'string'
            }
        )
    finally:
        conn.close()
    
    print(f"✓ Loaded {len(df):,} artists from staging")
    return df

def clean_year(year_str):
    """Extract year from various formats"""
    if pd.isna(year_str) or year_str == 'None' or year_str == '':
        return None
    
    try:        
        year = int(float(year_str))
        if 1000 <= year <= 2025:
            return year
        else:
            return None
    except:
        return None

def standardize_gender(gender_str):
    """Standardize gender values"""
    if pd.isna(gender_str) or gender_str == 'None' or gender_str == '':
        return 'Unknown'
    
    gender_lower = str(gender_str).lower().strip()
    
    if gender_lower in ['male', 'm', '(male)']:
        return 'Male'
    elif gender_lower in ['female', 'f', '(female)']:
        return 'Female'
    elif gender_lower in ['non-binary', 'nonbinary', 'nb', 'gender non-conforming', 'transgender woman', 'female transwoman']:
        return 'Non-binary'
    else:
        return 'Unknown'

def calculate_data_quality_score(row):
    """Calculate data quality score (0-1)"""
    score = 0.0
    
    # Birth year (20%)
    if pd.notna(row['birth_year']):
        score += 0.20
    
    # Nationality (20%)
    if pd.notna(row['nationality']) and row['nationality'] != 'Unknown':
        score += 0.20
    
    # Gender (15%)
    if pd.notna(row['gender_clean']) and row['gender_clean'] != 'Unknown':
        score += 0.15
    
    # Display name (15%)
    if pd.notna(row['display_name']) and len(str(row['display_name'])) > 0:
        score += 0.15
    
    # Death year for deceased artists (10%)
    if pd.notna(row['death_year']) or row['is_living']:
        score += 0.10
    
    # Wikidata enrichment (20%)
    if row['has_wikidata_enrichment']:
        score += 0.20
    
    return round(score, 2)

def transform_artists(df_artists, engine):
    """Apply all transformations to artist data"""
    print("\nTransforming artist data...")
    
    # 1. Clean years
    print("  Cleaning birth/death years...")
    df_artists['birth_year'] = df_artists['begin_date'].apply(clean_year)
    df_artists['death_year'] = df_artists['end_date'].apply(clean_year)
    
    # 2. Calculate is_living and lifespan
    df_artists['is_living'] = df_artists['death_year'].isna()
    df_artists['lifespan_years'] = df_artists.apply(
        lambda row: row['death_year'] - row['birth_year'] 
        if pd.notna(row['birth_year']) and pd.notna(row['death_year']) 
        else None, 
        axis=1
    )
    
    # 3. Standardize gender
    print("  Standardizing gender...")
    df_artists['gender_clean'] = df_artists['gender'].apply(standardize_gender)
    
    # 4. Add Wikidata enrichment
    print("  Adding Wikidata enrichment...")
    df_artists = add_wikidata_enrichment(df_artists, engine)
    
    # 5. Calculate data quality
    print("  Calculating data quality...")
    df_artists['data_quality_score'] = df_artists.apply(calculate_data_quality_score, axis=1)
    
    print(f"✓ Transformed {len(df_artists):,} artists")
    return df_artists

def add_wikidata_enrichment(df_artists, engine):
    """Add Wikidata enrichment data"""
    print("    Fetching Wikidata enrichments...")
    
    wikidata_query = """
        SELECT 
            wikidata_id,
            movements,
            birth_place,
            birth_place_country,
            death_place,
            education,
            influenced_by
        FROM staging.staging_wikidata_artists
        WHERE fetch_status = 'success'
    """
    
    conn = create_psycopg2_connection()
    try:
        df_wikidata = pd.read_sql_query(
            wikidata_query, 
            conn,
            dtype={
                'wikidata_id': 'string',
                'movements': 'object',
                'birth_place': 'string',
                'birth_place_country': 'string',
                'death_place': 'string',
                'education': 'string',
                'influenced_by': 'object'
            }
        )
    finally:
        conn.close()
    
    # Merge on wikidata_id (wiki_qid in staging)
    df_artists = df_artists.merge(
        df_wikidata,
        left_on='wiki_qid',
        right_on='wikidata_id',
        how='left'
    )
    
    # Add enrichment flag
    df_artists['has_wikidata_enrichment'] = df_artists['wikidata_id'].notna()
    
    # Rename column for consistency
    df_artists['art_movements'] = df_artists['movements']
    df_artists = df_artists.drop('movements', axis=1, errors='ignore')
    df_artists['artist_id'] = df_artists['constituent_id']

    return df_artists

def load_to_transformed(df, engine):
    print("\nLoading to transformed schema...")
    
    print(f"Available columns: {list(df.columns)}")
    
    columns_to_load = [
        'artist_id', 'display_name', 'birth_year', 'death_year', 'is_living',
        'lifespan_years', 'nationality', 'gender_clean', 'wikidata_id',
        'has_wikidata_enrichment', 'art_movements', 'birth_place',
        'birth_place_country', 'death_place', 'education', 'influenced_by',
        'data_quality_score'
    ]
    
    columns_to_load = [col for col in columns_to_load if col in df.columns]
    print(f"Loading columns: {columns_to_load}")
    
    df_load = df[columns_to_load].copy()
    
    # Clear existing data
    with engine.begin() as conn: # Use begin() for automatic commit
        conn.execute(text("TRUNCATE TABLE transformed.transformed_artists CASCADE"))
    
    # Create a fresh engine for to_sql
    from config import DB_CONNECTION_STRING
    from sqlalchemy import create_engine as make_engine
    write_engine = make_engine(DB_CONNECTION_STRING)
    
    df_load.to_sql(
        name='transformed_artists',
        schema='transformed',
        con=write_engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    write_engine.dispose()  # To close all connections of the connection pool
    
    print(f"✓ Loaded {len(df_load):,} transformed artists")

def verify_transformation(engine):
    """Verify transformed data"""
    print("\n" + "="*60)
    print("Verifying artist transformation...")
    print("="*60)
    
    queries = {
        'Total artists': 'SELECT COUNT(*) FROM transformed.transformed_artists',
        'Data quality distribution': '''
            SELECT 
                CASE 
                    WHEN data_quality_score >= 0.8 THEN 'Excellent (0.8-1.0)'
                    WHEN data_quality_score >= 0.6 THEN 'Good (0.6-0.8)'
                    WHEN data_quality_score >= 0.4 THEN 'Fair (0.4-0.6)'
                    ELSE 'Poor (0.0-0.4)'
                END as quality_tier,
                COUNT(*) as count,
                ROUND(AVG(data_quality_score), 2) as avg_score
            FROM transformed.transformed_artists
            GROUP BY quality_tier
            ORDER BY avg_score DESC
        ''',
        'Gender distribution': '''
            SELECT 
                gender_clean,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
            FROM transformed.transformed_artists
            GROUP BY gender_clean
            ORDER BY count DESC
        ''',
        'Top 10 nationalities': '''
            SELECT 
                nationality,
                COUNT(*) as artist_count
            FROM transformed.transformed_artists
            WHERE nationality != 'Unknown'
            GROUP BY nationality
            ORDER BY artist_count DESC
            LIMIT 10
        ''',
        'Wikidata enrichment rate': '''
            SELECT 
                has_wikidata_enrichment,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
            FROM transformed.transformed_artists
            GROUP BY has_wikidata_enrichment
        '''
    }
    
    conn = create_psycopg2_connection()
    try:
        for description, query in queries.items():
            print(f"\n{description}:")
            result = pd.read_sql_query(query, conn)
            print(result.to_string(index=False))
    finally:
        conn.close()

def main():
    print("="*60)
    print("ARTIST DATA TRANSFORMATION")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        # Connect to database
        engine = create_db_connection()
        print("✓ Connected to database\n")
        
        # Load staging data
        df_artists = load_staging_artists(engine)
        
        # Transform
        df_transformed = transform_artists(df_artists, engine)
        
        print("  Checking for duplicate artist IDs...")
        initial_count = len(df_transformed)
        df_transformed = df_transformed.drop_duplicates(subset=['constituent_id'])
        if len(df_transformed) < initial_count:
            print(f" >> Dropped {initial_count - len(df_transformed)} duplicate artist records")

        # Load to transformed schema
        load_to_transformed(df_transformed, engine)
        
        # Verify
        verify_transformation(engine)
        
        print("\n" + "="*60)
        print("TRANSFORMATION COMPLETE!")
        print("="*60)
        print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()

def run_transform_artists():
    """Airflow-callable function"""
    engine = create_db_connection()
    df_artists = load_staging_artists(engine)
    df_transformed = transform_artists(df_artists, engine)
    initial_count = len(df_transformed)
    df_transformed = df_transformed.drop_duplicates(subset=['constituent_id'])
    if len(df_transformed) < initial_count:
        print(f" >> Dropped {initial_count - len(df_transformed)} duplicate artist records")

    load_to_transformed(df_transformed, engine)
    return len(df_transformed)

if __name__ == "__main__":
    main()