"""
Transform Geography, Art Movements, and Economics Data
Populates transformation tables for geographic enrichment, art movement classification,
and economic/historical context for the MoMA project
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import os
import re
from difflib import SequenceMatcher
from pathlib import Path
import sys

# Load environment variables
load_dotenv()

# Add project root to Python path so we can import config
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DB_CONNECTION_STRING

def create_db_connection():
# Create database connection using SQLAlchemy with connection string from config.py
    return create_engine(DB_CONNECTION_STRING)

# ============================================================
# GEOGRAPHIC DATA TRANSFORMATION
# ============================================================

def load_staging_geographic_reference(engine):
    # Load geographic reference data from staging table
    print("Loading geographic reference from staging...")
    
    query = """
        SELECT 
            nationality,
            country_name,
            country_code_iso2,
            country_code_iso3,
            continent,
            region,
            subregion,
            latitude,
            longitude
        FROM staging.staging_geographic_reference
        WHERE country_name IS NOT NULL
        ORDER BY country_name
    """
    
    df = pd.read_sql(query, engine)
    print(f"✓ Loaded {len(df):,} geographic records from staging")
    return df

def transform_geography(engine):
    # Transform and load geography data
    print("\n" + "="*60)
    print("TRANSFORMING GEOGRAPHY DATA")
    print("="*60)
    
    # Load reference data from staging
    df_geo = load_staging_geographic_reference(engine)
    
    if len(df_geo) == 0:
        print("⚠ No geographic records found in staging table")
        return df_geo
    
    # Ensure nationality column is lowercase for consistent matching
    df_geo['nationality'] = df_geo['nationality'].str.lower().str.strip()
    
    print(f"\nProcessing {len(df_geo):,} geographic records...")
    
    # Calculate artist counts by nationality
    print("  Calculating artist counts by nationality...")
    with engine.connect() as conn:
        artist_counts = pd.read_sql("""
            SELECT 
                LOWER(TRIM(nationality)) as nationality,
                COUNT(DISTINCT artist_id) as total_moma_artists
            FROM transformed.transformed_artists
            WHERE nationality IS NOT NULL
            GROUP BY LOWER(TRIM(nationality))
        """, conn)
    
    # Merge artist counts
    df_geo = df_geo.merge(
        artist_counts,
        on='nationality',
        how='left'
    )
    df_geo['total_moma_artists'] = df_geo['total_moma_artists'].fillna(0).astype(int)
    
    # Calculate artwork counts by nationality
    # Count unique artworks for artists with this nationality
    print("  Calculating artwork counts by nationality...")
    with engine.connect() as conn:
        artwork_counts = pd.read_sql("""
            SELECT 
                LOWER(TRIM(ta.nationality)) as nationality,
                COUNT(DISTINCT tw.artwork_id) as total_moma_artworks
            FROM transformed.transformed_artists ta
            JOIN transformed.bridge_artwork_artist baa ON ta.artist_id = baa.artist_id
            JOIN transformed.transformed_artworks tw ON baa.artwork_id = tw.artwork_id
            WHERE ta.nationality IS NOT NULL
            GROUP BY LOWER(TRIM(ta.nationality))
        """, conn)
    
    # Merge artwork counts
    df_geo = df_geo.merge(
        artwork_counts,
        on='nationality',
        how='left'
    )
    df_geo['total_moma_artworks'] = df_geo['total_moma_artworks'].fillna(0).astype(int)
    
    # Add created_at timestamp
    df_geo['created_at'] = pd.Timestamp.now()
    
    print(f"✓ Transformed {len(df_geo):,} geographic records")
    print(f"  - Total matched artists across all countries: {df_geo['total_moma_artists'].sum():,}")
    print(f"  - Total matched artworks across all countries: {df_geo['total_moma_artworks'].sum():,}")
    
    return df_geo

def load_geography_to_db(df_geo, engine):
    """Load transformed geography to database"""
    print("\nLoading geography to transformed schema...")
    
    # Clear existing data
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE transformed.transformed_geography CASCADE"))
        conn.commit()
    
    # Select columns in correct order for database
    columns_to_load = [
        'nationality',
        'country_name',
        'country_code_iso2',
        'country_code_iso3',
        'continent',
        'region',
        'subregion',
        'latitude',
        'longitude',
        'total_moma_artists',
        'total_moma_artworks',
        'created_at'
    ]
    
    df_load = df_geo[columns_to_load].copy()
    
    # Load data
    df_load.to_sql(
        name='transformed_geography',
        schema='transformed',
        con=engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=500
    )
    
    print(f"✓ Loaded {len(df_load):,} geographic records")
# ============================================================
# ECONOMICS AND DATE CONTEXT TRANSFORMATION
# ============================================================

def load_staging_economic_data(engine):
    # Load economic data from staging table
    print("Loading economic data from staging...")
    
    query = """
        SELECT 
            year,
            gdp_usd_billions,
            inflation_rate_percent,
            unemployment_rate_percent,
            sp500_close,
            economic_period,
            major_events
        FROM staging.staging_economic_data
        WHERE year IS NOT NULL
        ORDER BY year ASC
    """
    
    df = pd.read_sql(query, engine)
    print(f"✓ Loaded {len(df):,} economic records from staging")
    return df

def transform_date_economics(engine):
    # Transform and load date/economics data
    print("\n" + "="*60)
    print("TRANSFORMING DATE & ECONOMICS DATA")
    print("="*60)
    
    # Load from staging
    df_econ = load_staging_economic_data(engine)
    
    if len(df_econ) == 0:
        print("⚠ No economic records found in staging table")
        return df_econ
    
    print(f"\nProcessing {len(df_econ):,} economic records...")
    
    # Create full_date (January 1 of each year)
    df_econ['full_date'] = pd.to_datetime(df_econ['year'].astype(str) + '-01-01')
    
    # Create date_key in YYYYMMDD format (YYYYMM01 for Jan 1)
    df_econ['date_key'] = df_econ['year'] * 10000 + 101  # YYYYMM01
    
    # Calculate decade (1920, 1930, 1940, etc.)
    df_econ['decade'] = (df_econ['year'] // 10) * 10
    
    # Calculate century (19, 20, 21)
    df_econ['century'] = df_econ['year'] // 100
    
    # Assign era based on historical periods
    print("  Assigning historical eras...")
    def assign_era(year):
        if year < 1914:
            return 'Gilded Age (rapid industrialisation & economic growth)'
        elif 1914 <= year <= 1918:
            return 'WWI'
        elif 1919 <= year <= 1928:
            return 'Roaring Twenties'
        elif 1929 <= year <= 1939:
            return 'Great Depression'
        elif 1940 <= year <= 1945:
            return 'WWII'
        elif 1946 <= year <= 1960:
            return 'Post-War Boom'
        elif 1961 <= year <= 1975:
            return 'Cold War'
        elif 1976 <= year <= 1990:
            return 'Postmodern'
        elif 1991 <= year <= 2007:
            return 'Digital Age'
        else:  # 2008+
            return 'Contemporary'
    
    df_econ['era'] = df_econ['year'].apply(assign_era)
    
    # Select and order columns for database
    columns_to_load = [
        'date_key',
        'full_date',
        'year',
        'decade',
        'century',
        'era',
        'gdp_usd_billions',
        'inflation_rate_percent',
        'unemployment_rate_percent',
        'sp500_close',
        'economic_period',
        'major_events'
    ]
    
    df_transformed = df_econ[columns_to_load].copy()
    
    print(f"✓ Transformed {len(df_transformed):,} date/economic records")
    print(f"  - Date range: {df_transformed['year'].min()} to {df_transformed['year'].max()}")
    print(f"  - Economic periods: {df_transformed['economic_period'].unique()}")
    print(f"  - Eras: {df_transformed['era'].unique()}")
    
    return df_transformed

def load_date_economics_to_db(df_date_econ, engine):
    # Load transformed date/economics to database
    print("\nLoading date/economics to transformed schema...")
    
    # Clear existing data
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE transformed.transformed_date_economics CASCADE"))
        conn.commit()
    
    # Load data
    df_date_econ.to_sql(
        name='transformed_date_economics',
        schema='transformed',
        con=engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=500
    )
    
    print(f"✓ Loaded {len(df_date_econ):,} year records with economic context")

# ============================================================
# VERIFICATION FUNCTIONS
# ============================================================

def verify_geographic_transform(engine):
    # Verify geographic transformation
    print("\n" + "="*60)
    print("VERIFYING GEOGRAPHY TRANSFORMATION")
    print("="*60)
    
    queries = {
        'Total geographic records': 
            'SELECT COUNT(*) as count FROM transformed.transformed_geography',
        
        'Records by continent': '''
            SELECT 
                continent,
                COUNT(*) as count,
                SUM(total_moma_artists) as total_moma_artists
            FROM transformed.transformed_geography
            WHERE continent IS NOT NULL
            GROUP BY continent
            ORDER BY total_moma_artists DESC
        ''',
        
        'Top 10 nationalities by artists': '''
            SELECT 
                nationality,
                country_name,
                continent,
                total_moma_artists
            FROM transformed.transformed_geography
            WHERE total_moma_artists > 0
            ORDER BY total_moma_artists DESC
            LIMIT 10
        '''
    }
    
    with engine.connect() as conn:
        for desc, query in queries.items():
            print(f"\n{desc}:")
            result = pd.read_sql(query, conn)
            print(result.to_string(index=False))

def verify_date_economics_transform(engine):
    # Verify date/economics transformation
    print("\n" + "="*60)
    print("VERIFYING DATE & ECONOMICS TRANSFORMATION")
    print("="*60)
    
    queries = {
        'Total years': 
            'SELECT COUNT(*) as count FROM transformed.transformed_date_economics',
        
        'Era distribution': '''
            SELECT 
                era,
                COUNT(*) as years,
                MIN(year) as start_year,
                MAX(year) as end_year
            FROM transformed.transformed_date_economics
            GROUP BY era
            ORDER BY start_year ASC
        ''',
        
        'War or recession years': '''
            SELECT 
                year,
                era,
                economic_period
            FROM transformed.transformed_date_economics
            WHERE era IN('WWI', 'WWII') OR economic_period = 'Recession'
            ORDER BY year ASC
        '''
    }
    
    with engine.connect() as conn:
        for desc, query in queries.items():
            print(f"\n{desc}:")
            result = pd.read_sql(query, conn)
            print(result.to_string(index=False))

# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    # Main execution function
    print("="*60)
    print("TRANSFORM GEOGRAPHY, MOVEMENTS & ECONOMICS DATA")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        # Connect to database
        engine = create_db_connection()
        print("✓ Connected to database\n")
        
        # GEOGRAPHY TRANSFORMATION
        df_geo = transform_geography(engine)
        load_geography_to_db(df_geo, engine)
        verify_geographic_transform(engine)
        
        # DATE & ECONOMICS TRANSFORMATION
        df_date_econ = transform_date_economics(engine)
        load_date_economics_to_db(df_date_econ, engine)
        verify_date_economics_transform(engine)
        
        # Summary
        print("\n" + "="*60)
        print("TRANSFORMATION COMPLETE!")
        print("="*60)
        print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        print("\n" + "="*60)
        print("SUMMARY")
        print("="*60)
        print(f"✓ Loaded {len(df_geo):,} geographic records")
        print(f"✓ Loaded {len(df_date_econ):,} year/economic records")
        print("\nNext steps:")
        print("1. Load transformed_artworks (if not already done)")
        print("2. Create dimensional model from transformed tables")
        print("3. Build fact tables with aggregations")
        print("4. Create analytical views and dashboards")
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()

def run_transform_geo():
    # Airflow-callable function
    engine = create_db_connection()
    df_geo = transform_geography(engine)
    load_geography_to_db(df_geo, engine)
    return len(df_geo)

def run_transform_economic():
    # Airflow-callable function
    engine = create_db_connection()
    df_date_econ = transform_date_economics(engine)
    load_date_economics_to_db(df_date_econ, engine)
    return len(df_date_econ)

if __name__ == "__main__":
    main()