"""
Build Bridge Tables
Creates many-to-many relationships between artworks and artists
"""

import pandas as pd
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

def build_artwork_artist_bridge(engine):
    #Build bridge table linking artworks to artists
    print("Building artwork-artist bridge table...")
    
    # Query to match artworks with artists via constituent_id(splitting rows with multiple constituent_id into table with regexp_split_to_table)
    query = """
    WITH artists_expanded AS (
        SELECT 
            tawks.artwork_id, 
            CAST(TRIM(value) AS INTEGER) AS artists	
        FROM transformed.transformed_artworks tawks 
        LEFT JOIN staging.staging_moma_artworks AS sa 
            on CAST(sa.object_id as integer) = tawks.artwork_id
        CROSS JOIN LATERAL regexp_split_to_table(sa.constituent_id, ',') AS value
        WHERE sa.constituent_id IS NOT NULL 
        AND sa.constituent_id != ''
    ),
    count_artists as (
        SELECT 
            count(artists) as artists_total,
            artwork_id
        FROM artists_expanded 
        GROUP BY artwork_id
    )
    SELECT
        artists_expanded.artwork_id, 
        tartst.artist_id,
        ca.artists_total
    FROM artists_expanded
        LEFT JOIN transformed.transformed_artists AS tartst on tartst.artist_id = artists_expanded.artists
        LEFT JOIN count_artists AS ca on artists_expanded.artwork_id = ca.artwork_id
        WHERE tartst.artist_id is not NULL
        ORDER BY artwork_id, artists_total
    """
    
    print("  Executing matching query...")
    df_bridge = pd.read_sql(query, engine)
    
    print(f"  Found {len(df_bridge):,} artwork-artist relationships")
    
    # Clear existing data
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE transformed.bridge_artwork_artist"))
        conn.commit()
    
    # Load bridge data
    print("  Loading bridge data...")
    df_bridge[['artwork_id', 'artist_id', 'artists_total']].to_sql(
        name='bridge_artwork_artist',
        schema='transformed',
        con=engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    print(f"✓ Built bridge table with {len(df_bridge):,} relationships")
    return df_bridge

def update_artist_stats(engine):
    # Update artist statistics based on artworks
    print("\nUpdating artist statistics...")
    
    update_query = """
    WITH artist_stats AS (
        SELECT 
            tar.artist_id,
            COUNT(DISTINCT baa.artwork_id) as artwork_count,
            MIN(ta.acquisition_year) as first_acquisition,
            MAX(ta.acquisition_year) as last_acquisition
        FROM transformed.transformed_artists tar
        LEFT JOIN transformed.bridge_artwork_artist baa 
            ON tar.artist_id = baa.artist_id
        LEFT JOIN transformed.transformed_artworks ta 
            ON baa.artwork_id = ta.artwork_id
        GROUP BY tar.artist_id
    )
    UPDATE transformed.transformed_artists tar
    SET 
        total_artworks_in_collection = COALESCE(ast.artwork_count, 0),
        first_acquisition_year = ast.first_acquisition,
        last_acquisition_year = ast.last_acquisition
    FROM artist_stats ast
    WHERE tar.artist_id = ast.artist_id
    """
    
    with engine.connect() as conn:
        conn.execute(text(update_query))
        conn.commit()
    
    print("✓ Updated artist statistics")

def verify_bridge_table(engine):
    # Verify bridge table
    print("\n" + "="*60)
    print("Verifying bridge table...")
    print("="*60)
    
    queries = {
        'Total relationships': 'SELECT COUNT(*) FROM transformed.bridge_artwork_artist',
        'Artworks with artists': '''
            SELECT COUNT(DISTINCT artwork_id) as artworks_with_artists
            FROM transformed.bridge_artwork_artist
        ''',
        'Artists with artworks': '''
            SELECT COUNT(DISTINCT artist_id) as artists_with_artworks
            FROM transformed.bridge_artwork_artist
        ''',
        'Collaborations (multi-artist works)': '''
            SELECT 
                artwork_id,
                COUNT(artist_id) as artist_count
            FROM transformed.bridge_artwork_artist
            GROUP BY artwork_id
            HAVING COUNT(artist_id) > 1
            ORDER BY artist_count DESC
            LIMIT 10
        ''',
        'Most prolific artists': '''
            SELECT 
                tar.display_name,
                tar.total_artworks_in_collection,
                tar.first_acquisition_year,
                tar.last_acquisition_year
            FROM transformed.transformed_artists tar
            WHERE tar.total_artworks_in_collection > 0
            ORDER BY tar.total_artworks_in_collection DESC
            LIMIT 10
        '''
    }
    
    with engine.connect() as conn:
        for description, query in queries.items():
            print(f"\n{description}:")
            result = pd.read_sql(query, conn)
            print(result.to_string(index=False))

def main():
    print("="*60)
    print("BRIDGE TABLE CONSTRUCTION")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        # Connect to database
        engine = create_db_connection()
        print("✓ Connected to database\n")
        
        # Build bridge table
        df_bridge = build_artwork_artist_bridge(engine)
        
        # Update artist statistics
        update_artist_stats(engine)
        
        # Verify
        verify_bridge_table(engine)
        
        print("\n" + "="*60)
        print("BRIDGE TABLE COMPLETE!")
        print("="*60)
        print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()

def run_build_bridge_add_stats():
    # Airflow-callable function
    engine = create_db_connection()
    df_bridge = build_artwork_artist_bridge(engine)
    update_artist_stats(engine)
    return len(df_bridge)

if __name__ == "__main__":
    main()