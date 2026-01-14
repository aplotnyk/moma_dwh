"""
Load Dimensional Layer
Loads dimension and fact tables from transformed layer
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import os
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
# DIM_DATE
# ============================================================

def load_dim_date(engine):
    # Load dim_date from transformed_date_economics
    print("\n" + "="*60)
    print("LOADING DIM_DATE")
    print("="*60)
    
    print("Loading date data from transformed layer...")
    query = """
        SELECT 
            date_key,
            full_date,
            year,
            decade,
            century,
            era,
            gdp_usd_billions,
            inflation_rate_percent,
            unemployment_rate_percent,
            sp500_close,
            economic_period,
            major_events
        FROM transformed.transformed_date_economics
        ORDER BY year
    """
    
    df_date = pd.read_sql(query, engine)
    print(f"✓ Loaded {len(df_date):,} date records")
    
    print("Loading to dimensional.dim_date...")
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE dimensional.dim_date CASCADE"))
        conn.commit()
    
    df_date.to_sql(
        name='dim_date',
        schema='dimensional',
        con=engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=500
    )
    
    print(f"✓ Loaded {len(df_date):,} dimension records")
    return len(df_date)

# ============================================================
# DIM_GEOGRAPHY
# ============================================================

def load_dim_geography(engine):
    # Load dim_geography from transformed_geography
    print("\n" + "="*60)
    print("LOADING DIM_GEOGRAPHY")
    print("="*60)
    
    print("Loading geography data from transformed layer...")
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
            longitude,
            total_moma_artists as total_artists,
            total_moma_artworks as total_artworks
        FROM transformed.transformed_geography
        ORDER BY country_name
    """
    
    df_geo = pd.read_sql(query, engine)
    print(f"✓ Loaded {len(df_geo):,} geography records from transformed")
    

    print("Loading to dimensional.dim_geography...")
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE dimensional.dim_geography CASCADE"))
        conn.commit()
    
    df_geo.to_sql(
        name='dim_geography',
        schema='dimensional',
        con=engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=500
    )
    
    print(f"✓ Loaded {len(df_geo):,} dimension records")
    return len(df_geo)

# ============================================================
# DIM_ARTIST (SCD Type 0 - No History)
# ============================================================

def load_dim_artist(engine):
    import pandas as pd
    from sqlalchemy import text

    print("\n" + "="*60)
    print("LOADING DIM_ARTIST")
    print("="*60)
    
    print("Loading artist data from transformed layer...")
    query = """
        SELECT 
            artist_id,
            display_name,
            birth_year,
            death_year,
            is_living,
            lifespan_years,
            nationality,
            birth_place,
            death_place,
            gender_clean as gender,
            wikidata_id,
            art_movements,
            education,
            influenced_by,
            total_artworks_in_collection as total_artworks,
            first_acquisition_year,
            last_acquisition_year,
            data_quality_score,
            has_wikidata_enrichment
        FROM transformed.transformed_artists
        ORDER BY artist_id
    """
    
    df_artist = pd.read_sql(query, engine)
    print(f"✓ Loaded {len(df_artist):,} artist records from transformed")

    # Reorder columns
    columns = [
        'artist_id', 'display_name', 'birth_year', 'death_year', 'is_living',
        'lifespan_years', 'nationality',
        'birth_place', 'death_place', 'gender', 'wikidata_id',
        'art_movements', 'education', 'influenced_by',
        'total_artworks', 'first_acquisition_year', 'last_acquisition_year',
        'data_quality_score', 'has_wikidata_enrichment'
    ]
    df_artist = df_artist[columns]

    df_artist['nationality'] = df_artist['nationality'].str.lower()

    unknown_artist = {
        'artist_id': 0,
        'display_name': 'Unknown Artist',
        'birth_year': None,
        'death_year': None,
        'is_living': None,
        'lifespan_years': None,
        'nationality': None,
        'birth_place': None,
        'death_place': None,
        'gender': None,
        'wikidata_id': None,
        'art_movements': None,
        'education': None,
        'influenced_by': None,
        'total_artworks': 0,
        'first_acquisition_year': None,
        'last_acquisition_year': None,
        'data_quality_score': 0,
        'has_wikidata_enrichment': False
    }

    df_unknown = pd.DataFrame([unknown_artist])
    df_artist = pd.concat([df_unknown, df_artist], ignore_index=True)

    print("Loading to dimensional.dim_artist...")
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE dimensional.dim_artist CASCADE"))
        conn.commit()
    
    df_artist.to_sql(
        name='dim_artist',
        schema='dimensional',
        con=engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=500
    )
    
    print(f"✓ Loaded {len(df_artist):,} artist dimension records")
    return len(df_artist)

# ============================================================
# DIM_ARTWORK
# ============================================================

def load_dim_artwork(engine):
    # Load dim_artwork from transformed_artworks
    print("\n" + "="*60)
    print("LOADING DIM_ARTWORK")
    print("="*60)
    
    print("Loading artwork data from transformed layer...")
    query = """
        SELECT 
            artwork_id,
            title,
            accession_number,
            medium,
            classification,
            department,
            height_cm,
            width_cm,
            depth_cm,
            dimensions_text,
            creation_year,
            creation_date_raw,
            date_certainty,
            acquisition_method,
            credit_line,
            cataloged,
            moma_url,
            image_url,
            data_quality_score
        FROM transformed.transformed_artworks
        ORDER BY artwork_id
    """
    
    df_artwork = pd.read_sql(query, engine)
    print(f"✓ Loaded {len(df_artwork):,} artwork records")
    
    # Calculate creation_decade and creation_century
    df_artwork['creation_decade'] = (df_artwork['creation_year'] // 10 * 10).where(
        df_artwork['creation_year'].notna(), None
    )
    df_artwork['creation_century'] = (df_artwork['creation_year'] // 100).where(
        df_artwork['creation_year'].notna(), None
    )
    
    # Reorder columns
    columns = [
        'artwork_id', 'title', 'accession_number', 'medium', 'classification',
        'department', 'height_cm', 'width_cm', 'depth_cm', 'dimensions_text',
        'creation_year', 'creation_date_raw', 'date_certainty',
        'creation_decade', 'creation_century',
        'acquisition_method', 'credit_line', 'cataloged',
        'moma_url', 'image_url', 'data_quality_score'
    ]
    df_artwork = df_artwork[columns]
    
    print("Loading to dimensional.dim_artwork...")
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE dimensional.dim_artwork CASCADE"))
        conn.commit()
    
    df_artwork.to_sql(
        name='dim_artwork',
        schema='dimensional',
        con=engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=500
    )
    
    print(f"✓ Loaded {len(df_artwork):,} artwork dimension records")
    return len(df_artwork)

# ============================================================
# FACT_ARTWORK_ACQUISITIONS
# ============================================================
def load_fact_artwork_acquisitions(engine):
    # Load fact_artwork_acquisitions
    print("\n" + "="*60)
    print("LOADING FACT_ARTWORK_ACQUISITIONS")
    print("="*60)
    
    print("Building fact table from transformed data...")
    
    # Complex query joining artworks, artists, and dates
    query = """
 		WITH artwork_artist_data AS (
            SELECT 
                ta.artwork_id,
                ta.acquisition_year,
                ta.creation_year,
                ta.accession_number,
                ta.acquisition_method,
                ta.cataloged,
                tw.artist_id,
                tw.nationality,
                tw.birth_year,
                tw.death_year,
                tw.is_living,
                CASE WHEN tw.gender_clean = 'Female' THEN TRUE ELSE FALSE END as is_female_artist
            FROM transformed.transformed_artworks ta
            LEFT JOIN transformed.bridge_artwork_artist baa ON ta.artwork_id = baa.artwork_id
            LEFT JOIN transformed.transformed_artists tw ON baa.artist_id = tw.artist_id
        )
       
        SELECT 
            aad.artwork_id,
            COALESCE(aad.artist_id, 0) as artist_id,
            dae.date_key as acquisition_date_key,
            dce.date_key as creation_date_key,
            aad.nationality,
            aad.accession_number,
            CASE when (aad.acquisition_year - aad.creation_year) < 0 THEN 0
            	ELSE (aad.acquisition_year - aad.creation_year) END as years_from_creation_to_acquisition,
            CASE WHEN LOWER(aad.acquisition_method) = 'gift' THEN TRUE ELSE FALSE END as is_gift,
            CASE WHEN LOWER(aad.acquisition_method) = 'purchase' THEN TRUE ELSE FALSE END as is_purchase,
            CASE WHEN LOWER(aad.acquisition_method) = 'bequest' THEN TRUE ELSE FALSE END as is_bequest,
            aad.is_living as is_living_artist_at_acquisition,
            CASE WHEN (aad.acquisition_year - aad.creation_year) <= 20 THEN TRUE ELSE FALSE END as is_contemporary,
            aad.cataloged as is_cataloged,
            aad.is_female_artist
        FROM artwork_artist_data aad
        LEFT JOIN dimensional.dim_date dae ON aad.acquisition_year = dae.year
        LEFT JOIN dimensional.dim_date dce ON aad.creation_year = dce.year
    """
    
    df_fact = pd.read_sql(query, engine)
    print(f"✓ Built fact table with {len(df_fact):,} records")
    
    # Clear and load
    print("Loading to dimensional.fact_artwork_acquisitions...")
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE dimensional.fact_artwork_acquisitions CASCADE"))
        conn.commit()
    
    df_fact.to_sql(
        name='fact_artwork_acquisitions',
        schema='dimensional',
        con=engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=500
    )
    
    print(f"✓ Loaded {len(df_fact):,} fact records")
    return len(df_fact)
# ============================================================
# FACT_ARTIST_SUMMARY
# ============================================================

def load_fact_artist_summary(engine):
    """Load fact_artist_summary"""
    print("\n" + "="*60)
    print("LOADING FACT_ARTIST_SUMMARY")
    print("="*60)
    
    print("Building artist summary fact table...")
    
    query = """
      WITH artist_artwork_stats AS (
            SELECT 
                da.artist_id,
                da.nationality,
                COUNT(DISTINCT dw.artwork_id) as total_artworks,
                MIN(dw.creation_year) as earliest_artwork_year,
                MAX(dw.creation_year) as latest_artwork_year,
                MIN(CASE WHEN dae.year IS NOT NULL THEN dae.year END) as first_acquisition_year,
                MAX(CASE WHEN dae.year IS NOT NULL THEN dae.year END) as last_acquisition_year,
                ROUND(AVG(faa.years_from_creation_to_acquisition)::NUMERIC, 0) as avg_years_to_acquisition,
                COUNT(DISTINCT dw.artwork_id) FILTER (WHERE dw.creation_year IS NOT NULL AND 
                    (date_part('year', CURRENT_DATE) - dw.creation_year) <= 20) as contemporary_works_count
            FROM dimensional.dim_artist da
            LEFT JOIN dimensional.fact_artwork_acquisitions faa ON da.artist_id = faa.artist_id
            LEFT JOIN dimensional.dim_artwork dw ON faa.artwork_id = dw.artwork_id
            LEFT JOIN dimensional.dim_date dae ON faa.acquisition_date_key = dae.date_key
            GROUP BY da.artist_id, da.nationality
        )
        SELECT 
            artist_id,
            nationality,
            CURRENT_DATE as snapshot_date, 
            total_artworks,
            first_acquisition_year,
            last_acquisition_year,
            (last_acquisition_year - first_acquisition_year) as span_of_acquisitions_years,
            earliest_artwork_year,
            latest_artwork_year,
            avg_years_to_acquisition,
            contemporary_works_count > 0 as has_contemporary_works
        FROM artist_artwork_stats
    """
    
    df_fact_summary = pd.read_sql(query, engine)
    print(f"✓ Built artist summary with {len(df_fact_summary):,} records")
    
    # Clear and load
    print("Loading to dimensional.fact_artist_summary...")
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE dimensional.fact_artist_summary CASCADE"))
        conn.commit()
    
    df_fact_summary.to_sql(
        name='fact_artist_summary',
        schema='dimensional',
        con=engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=500
    )
    
    print(f"✓ Loaded {len(df_fact_summary):,} artist summary records")
    return len(df_fact_summary)

# ============================================================
# VERIFICATION
# ============================================================

def verify_dimensional_load(engine):
    # Verify dimensional layer loads
    print("\n" + "="*60)
    print("VERIFYING DIMENSIONAL LAYER")
    print("="*60)
    
    queries = {
        'Sample geographic analysis (by nationality)': '''
            SELECT 
                dg.nationality,
                dg.country_name,
                dg.continent,
                dg.total_artists,
                dg.total_artworks
            FROM dimensional.dim_geography dg
            WHERE dg.total_artists > 0
            ORDER BY dg.total_artists DESC
            LIMIT 10
        ''',
        'Sample artist with movement analysis': '''
            SELECT 
                da.display_name,
                dg.country_name,
                da.art_movements,
                fas.total_artworks
            FROM dimensional.dim_artist da
            LEFT JOIN dimensional.dim_geography dg ON da.nationality = dg.nationality
            LEFT JOIN dimensional.fact_artist_summary fas ON da.artist_id = fas.artist_id
            WHERE fas.total_artworks > 20
            ORDER BY fas.total_artworks DESC
            LIMIT 10
        ''',
        'Dimension record counts': '''
            SELECT 
                (SELECT COUNT(*) FROM dimensional.dim_date) as date_records,
                (SELECT COUNT(*) FROM dimensional.dim_geography) as geography_records,
                (SELECT COUNT(*) FROM dimensional.dim_artist) as artist_records,
                (SELECT COUNT(*) FROM dimensional.dim_artwork) as artwork_records
        '''
        ,
        'Fact table record counts': '''
            SELECT 
                (SELECT COUNT(*) FROM dimensional.fact_artwork_acquisitions) as acquisitions,
                (SELECT COUNT(*) FROM dimensional.fact_artist_summary) as artist_summaries
        '''
    }
    
    with engine.connect() as conn:
        for desc, query in queries.items():
            print(f"\n{desc}:")
            result = pd.read_sql(query, conn)
            print(result.to_string(index=False))

# ============================================================
# MAIN
# ============================================================

def main():
    print("="*60)
    print("DIMENSIONAL LAYER LOADER")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        engine = create_db_connection()
        print("✓ Connected to database\n")
        
        # Load dimensions in order
        counts = {}
        counts['dim_date'] = load_dim_date(engine)
        counts['dim_geography'] = load_dim_geography(engine)
        counts['dim_artist'] = load_dim_artist(engine)
        counts['dim_artwork'] = load_dim_artwork(engine)
        
        # # Load facts
        counts['fact_acquisitions'] = load_fact_artwork_acquisitions(engine)
        counts['fact_summary'] = load_fact_artist_summary(engine)
        
        # Verify
        verify_dimensional_load(engine)
        
        # Summary
        print("\n" + "="*60)
        print("DIMENSIONAL LAYER LOAD COMPLETE!")
        print("="*60)
        print("\nDimensions loaded:")
        print(f"  ✓ dim_date: {counts['dim_date']:,} records")
        print(f"  ✓ dim_geography: {counts['dim_geography']:,} records (keyed by nationality)")
        print(f"  ✓ dim_artist (SCD Type 0): {counts['dim_artist']:,} records (FK to geography via nationality)")
        print(f"  ✓ dim_artwork: {counts['dim_artwork']:,} records")
        print("\nFact tables loaded:")
        print(f"  ✓ fact_artwork_acquisitions: {counts['fact_acquisitions']:,} records")
        print(f"  ✓ fact_artist_summary: {counts['fact_summary']:,} records")
        print(f"\n  ℹ  Geographic analysis via nationality column")
        print(f"  ℹ  Art movements in dim_artist.art_movements (TEXT[] array)")
        print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()

# ============================================================
# AIRFLOW CALLABLE FUNCTIONS
# ============================================================

def run_load_dim_date():
    """Airflow callable for loading dim_date"""
    engine = create_db_connection()
    return load_dim_date(engine)

def run_load_dim_geography():
    """Airflow callable for loading dim_geography"""
    engine = create_db_connection()
    return load_dim_geography(engine)

def run_load_dim_artist():
    """Airflow callable for loading dim_artist"""
    engine = create_db_connection()
    return load_dim_artist(engine)

def run_load_dim_artwork():
    """Airflow callable for loading dim_artwork"""
    engine = create_db_connection()
    return load_dim_artwork(engine)

def run_load_fact_artwork_acquisitions():
    """Airflow callable for loading fact_artwork_acquisitions"""
    engine = create_db_connection()
    return load_fact_artwork_acquisitions(engine)

def run_load_fact_artist_summary():
    """Airflow callable for loading fact_artist_summary"""
    engine = create_db_connection()
    return load_fact_artist_summary(engine)

def run_verify_dimensional():
    """Airflow callable for verification"""
    engine = create_db_connection()
    verify_dimensional_load(engine)
    return True

if __name__ == "__main__":
    main()