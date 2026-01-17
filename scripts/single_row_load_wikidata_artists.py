"""
Load Wikidata Artist Enrichment Data
Enriches MoMA artists with biographical data from Wikidata
"""

import pandas as pd
import requests
from sqlalchemy import create_engine, text
from datetime import datetime
import os
import time
import json # for data formatting
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DB_CONNECTION_STRING, WIKIDATA_USER_AGENT

def create_db_connection():
# Create database connection using SQLAlchemy with connection string from config.py
    return create_engine(DB_CONNECTION_STRING)

# Get MoMA artists that have Wikidata IDs
def get_moma_artists_with_wikidata(engine):
    print("Loading MoMA artists with Wikidata IDs...")
    
    query = """
        SELECT 
            display_name,
            wiki_qid,
            nationality,
            begin_date,
            end_date
        FROM staging.staging_moma_artists
        WHERE wiki_qid IS NOT NULL 
          AND wiki_qid != ''
          AND wiki_qid != 'None'
--        LIMIT 100  -- Start with top 100 for testing
    """
    
    df = pd.read_sql(query, engine)
    print(f"✓ Found {len(df)} artists with Wikidata IDs")
    return df

def drop_duplicate_wikidata_ids(df):
    # Removes all rows with duplicated wiki_qid
    print("\nChecking for duplicate wikidata_id values...")

    # Identify duplicates (returns True for any repeated value)
    duplicate_mask = df['wiki_qid'].duplicated(keep=False)

    if duplicate_mask.any():
        duplicates = df.loc[duplicate_mask, 'wiki_qid']

        print(f"⚠️ Found {duplicates.nunique()} duplicated wikidata_id values.")
        print("Example duplicate IDs:")
        print(duplicates.value_counts().head())

        # Drop ALL rows where wiki_qid is duplicated
        df_clean = df[~duplicate_mask].copy()

        print(f"✓ Removed {len(df) - len(df_clean)} rows with non-unique wikidata_id values.")
        print(f"✓ Remaining rows: {len(df_clean)}")

        return df_clean
    else:
        print("✓ No duplicate wikidata_id values found.")
        return df


# Fetch detailed information from Wikidata for an artist
def fetch_wikidata_info(wikidata_id):    
    # Wikidata SPARQL endpoint
    url = "https://query.wikidata.org/sparql"
    
    # SPARQL query to get artist information
    query = f"""
    SELECT ?item ?itemLabel 
           ?birthPlace ?birthPlaceLabel 
           ?birthCountry ?birthCountryLabel
           ?deathPlace ?deathPlaceLabel 
           ?movement ?movementLabel 
           ?education ?educationLabel 
           ?influencedBy ?influencedByLabel
           ?bioDescription
    WHERE {{
      BIND(wd:{wikidata_id} AS ?item)
      
      OPTIONAL {{ 
        ?item wdt:P19 ?birthPlace.
        OPTIONAL {{ ?birthPlace wdt:P17 ?birthCountry. }}
      }}
      
      OPTIONAL {{ ?item wdt:P20 ?deathPlace. }}
      
      OPTIONAL {{ ?item wdt:P135 ?movement. }}
      
      OPTIONAL {{ ?item wdt:P69 ?education. }}
      
      OPTIONAL {{ ?item wdt:P737 ?influencedBy. }}
      
      OPTIONAL {{ 
        ?item schema:description ?bioDescription.
        FILTER(LANG(?bioDescription) = "en")
      }}
      
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    LIMIT 20
    """
    
    headers = {
        "User-Agent": WIKIDATA_USER_AGENT,
        "Accept": "application/json"
    }
    
    try:
        response = requests.get(
            url, 
            params={'query': query, 'format': 'json'},
            headers=headers,
            timeout=10 # waiting max 10sec - freezing prevention
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return None

# Parse Wikidata API response into structured data
def parse_wikidata_response(data, artist_name):    
    if not data or 'results' not in data or 'bindings' not in data['results']:
        return None
    
    bindings = data['results']['bindings']
    
    if not bindings:
        return None
    
    # Extract information
    movements = []
    educations = []
    influenced_by_artists = []
    birth_place = None
    birth_country = None
    death_place = None
    bio = None
    
    for binding in bindings:
        # Art movements
        if 'movementLabel' in binding:
            movement = binding['movementLabel']['value']
            if movement and movement not in movements:
                movements.append(movement)
        
        # Education
        if 'educationLabel' in binding:
            education = binding['educationLabel']['value']
            if education and education not in educations:
                educations.append(education)
        
        # Influenced by
        if 'influencedByLabel' in binding:
            influenced_by = binding['influencedByLabel']['value']
            if influenced_by and influenced_by not in influenced_by_artists:
                influenced_by_artists.append(influenced_by)
        
        # Birth place
        if 'birthPlaceLabel' in binding and not birth_place:
            birth_place = binding['birthPlaceLabel']['value']
        
        # Birth country (extracted separately)
        if 'birthCountryLabel' in binding and not birth_country:
            birth_country = binding['birthCountryLabel']['value']
        
        # Death place
        if 'deathPlaceLabel' in binding and not death_place:
            death_place = binding['deathPlaceLabel']['value']
        
        # Biography
        if 'bioDescription' in binding and not bio:
            bio = binding['bioDescription']['value']
    
    return {
        'movements': movements if movements else None,
        'education': ', '.join(educations) if educations else None,
        'influenced_by': influenced_by_artists if influenced_by_artists else None,
        'birth_place': birth_place,
        'birth_place_country': birth_country,
        'death_place': death_place,
        'bio_text': bio
    }

# Enrich artists with Wikidata information
def enrich_artists(artists_df):
    print("\nEnriching artists with Wikidata...")
    print("(This will take longer due to rate limiting)")
    
    enriched_data = []
    
    for idx, row in artists_df.iterrows():
        artist_name = row['display_name']
        wikidata_id = row['wiki_qid']
        
        print(f"  [{idx+1}/{len(artists_df)}] {artist_name} ({wikidata_id})")
        
        # Fetch from Wikidata
        response = fetch_wikidata_info(wikidata_id)
        
        if response:
            parsed = parse_wikidata_response(response, artist_name)
            
            if parsed:
                enriched_data.append({
                    'artist_name': artist_name,
                    'wikidata_id': wikidata_id,
                    'movements': parsed['movements'],
                    'bio_text': parsed['bio_text'],
                    'birth_place': parsed['birth_place'],
                    'birth_place_country': parsed['birth_place_country'],
                    'death_place': parsed['death_place'],
                    'education': parsed['education'],
                    'influenced_by': parsed['influenced_by'],
                    'api_response_json': json.dumps(response), # Convert API response(Python dict) into a JSON string
                    'fetch_status': 'success'
                })
                print(f"    ✓ Enriched successfully")
            else:
                enriched_data.append({
                    'artist_name': artist_name,
                    'wikidata_id': wikidata_id,
                    'movements': None,
                    'bio_text': None,
                    'birth_place': None,
                    'birth_place_country': None,
                    'death_place': None,
                    'education': None,
                    'influenced_by': None,
                    'api_response_json': None,
                    'fetch_status': 'no_data'
                })
                print(f"    ⚠️  No data found")
        else:
            enriched_data.append({
                'artist_name': artist_name,
                'wikidata_id': wikidata_id,
                'movements': None,
                'bio_text': None,
                'birth_place': None,
                'birth_place_country': None,
                'death_place': None,
                'education': None,
                'influenced_by': None,
                'api_response_json': None,
                'fetch_status': 'failed'
            })
            print(f"    ❌ API request failed")
        
        time.sleep(0.05) # Rate limiting - pauses execution for 0.05 seconds
    
    df = pd.DataFrame(enriched_data)
    print(f"\n✓ Enriched {len(df)} artist records")
    return df

def load_to_database(df, engine):
    """Load Wikidata enrichment data to staging table"""
    print("\nLoading data to PostgreSQL...")
    
    # Clear existing data
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE staging.staging_wikidata_artists"))
        conn.commit()
    
    # Load data
    df.to_sql(
        name='staging_wikidata_artists',
        schema='staging',
        con=engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=500 # batch insert 500 rows at a time
    )
    
    print(f"✓ Loaded {len(df)} Wikidata enrichment records")

def verify_load(engine):
    """Verify data was loaded correctly"""
    print("\n" + "="*60)
    print("Verifying Wikidata enrichment load...")
    print("="*60)
    
    queries = {
        'Total artists enriched': 'SELECT COUNT(*) FROM staging.staging_wikidata_artists',
        'Enrichment success rate': '''
            SELECT 
                fetch_status,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM staging.staging_wikidata_artists
            GROUP BY fetch_status
        ''',
        'Artists with movements': '''
            SELECT 
                artist_name,
                movements,
                birth_place,
                birth_place_country
            FROM staging.staging_wikidata_artists
            WHERE movements IS NOT NULL
            LIMIT 10
        ''',
        'Artists with influences': '''
            SELECT 
                artist_name,
                influenced_by
            FROM staging.staging_wikidata_artists
            WHERE influenced_by IS NOT NULL
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
    print("WIKIDATA ARTIST ENRICHMENT LOADER")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        # Connect to database
        print("Connecting to PostgreSQL...")
        engine = create_db_connection()
        print("✓ Connected successfully\n")
        
        # Get artists with Wikidata IDs
        artists_df = get_moma_artists_with_wikidata(engine)
        
        if artists_df.empty:
            print("⚠️  No artists with Wikidata IDs found")
            return
        
        # Drop duplicate wikidata IDs
        artists_df = drop_duplicate_wikidata_ids(artists_df)

        if artists_df.empty:
            print("⚠️  All rows removed due to duplicated wikidata_id values. Nothing to process.")
            return
        
        # Enrich with Wikidata
        enriched_df = enrich_artists(artists_df)
        
        # Load to database
        load_to_database(enriched_df, engine)
        
        # Verify
        verify_load(engine)
        
        print("\n" + "="*60)
        print("LOAD COMPLETE!")
        print("="*60)
        print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("\nNote: This script processed all artists with Wikidata ID.")
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()

def fetch_and_load_wikidata():
    # Airflow-callable function
    engine = create_db_connection()
    artists_df = get_moma_artists_with_wikidata(engine)
    artists_df = drop_duplicate_wikidata_ids(artists_df)
    enriched_df = enrich_artists(artists_df)
    load_to_database(enriched_df, engine)
    return len(enriched_df)

if __name__ == "__main__":
    main()