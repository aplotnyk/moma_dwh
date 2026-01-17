"""
Load Wikidata Artist Enrichment Data
Enriches MoMA artists with biographical data from Wikidata using batch SPARQL queries
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


# def create_db_connection():
#     """ Create database connection with connection pooling for better performance"""
#     engine = create_engine(
#         DB_CONNECTION_STRING,
#         pool_size=10,
#         max_overflow=20,
#         pool_pre_ping=True,
#         connect_args={
#             'connect_timeout': 10,
#             'options': '-c statement_timeout=30000'
#         }
#     )
#     return engine

def create_db_connection():
# Create database connection using SQLAlchemy with connection string from config.py
    return create_engine(DB_CONNECTION_STRING)


def get_moma_artists_with_wikidata(engine):
    # Get MoMA artists that have Wikidata IDs
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
    """
    
    df = pd.read_sql(query, engine)
    print(f"✓ Found {len(df):,} artists with Wikidata IDs")
    return df


def drop_duplicate_wikidata_ids(df):
    # Remove all rows with duplicated wiki_qid
    print("\nChecking for duplicate wikidata_id values...")

    # Identify duplicates (returns True for any repeated value)
    duplicate_mask = df['wiki_qid'].duplicated(keep=False)

    if duplicate_mask.any():
        duplicates = df.loc[duplicate_mask, 'wiki_qid']

        print(f"⚠️  Found {duplicates.nunique()} duplicated wikidata_id values.")
        print("Example duplicate IDs:")
        print(duplicates.value_counts().head())

        # Drop ALL rows where wiki_qid is duplicated
        df_clean = df[~duplicate_mask].copy()

        print(f"✓ Removed {len(df) - len(df_clean)} rows with non-unique wikidata_id values.")
        print(f"✓ Remaining rows: {len(df_clean):,}")

        return df_clean
    else:
        print("✓ No duplicate wikidata_id values found.")
        return df


def fetch_wikidata_batch(wikidata_ids):
    # Fetch multiple artists in ONE SPARQL query

     # Wikidata SPARQL endpoint
    url = "https://query.wikidata.org/sparql"
    
    # Build VALUES clause for batch query
    # ['Q5582', 'Q5593'] becomes "wd:Q5582 wd:Q5593"
    values_clause = " ".join([f"wd:{qid}" for qid in wikidata_ids])
    
    # SPARQL query that fetches MULTIPLE artists at once
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
      VALUES ?item {{ {values_clause} }}
      
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
            timeout=30  # waiting max 30sec - freezing prevention(extended for batch query)
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        print(f"    ⚠️  Query timeout (batch size might be too large)")
        return None
    except requests.exceptions.RequestException as e:
        print(f"    ❌ Request failed: {e}")
        return None
    except Exception as e:
        print(f"    ❌ Unexpected error: {e}")
        return None


def parse_batch_response(response_json, wikidata_ids):
    # Parse batch response and organize data by artist

    if not response_json or 'results' not in response_json:
        return {}
    
    bindings = response_json['results'].get('bindings', [])
    
    if not bindings:
        return {}
    
    # Initialize data structure for each artist
    artists_data = {}
    for wikidata_id in wikidata_ids:
        artists_data[wikidata_id] = {
            'movements': [],
            'education': [],
            'influenced_by': [],
            'birth_place': None,
            'birth_place_country': None,
            'death_place': None,
            'bio_text': None
        }
    
    # Process each row in the response
    for binding in bindings:
        # Get the artist ID from the binding
        if 'item' not in binding:
            continue
            
        item_uri = binding['item']['value']
        # Extract Q-ID from URI ex.: "http://www.wikidata.org/entity/Q5582"
        wikidata_id = item_uri.split('/')[-1]
        
        # Skip if this artist wasn't in the query (normally shouldn't happen)
        if wikidata_id not in artists_data:
            continue
        
        artist_data = artists_data[wikidata_id]
        
        # Collect movements array
        if 'movementLabel' in binding:
            movement = binding['movementLabel']['value']
            if movement and movement not in artist_data['movements']:
                artist_data['movements'].append(movement)
        
        # Collect education array
        if 'educationLabel' in binding:
            education = binding['educationLabel']['value']
            if education and education not in artist_data['education']:
                artist_data['education'].append(education)
        
        # Collect influences array
        if 'influencedByLabel' in binding:
            influenced_by = binding['influencedByLabel']['value']
            if influenced_by and influenced_by not in artist_data['influenced_by']:
                artist_data['influenced_by'].append(influenced_by)
        
        # Single-value fields (take first occurrence)
        if 'birthPlaceLabel' in binding and not artist_data['birth_place']:
            artist_data['birth_place'] = binding['birthPlaceLabel']['value']
        
        if 'birthCountryLabel' in binding and not artist_data['birth_place_country']:
            artist_data['birth_place_country'] = binding['birthCountryLabel']['value']
        
        if 'deathPlaceLabel' in binding and not artist_data['death_place']:
            artist_data['death_place'] = binding['deathPlaceLabel']['value']
        
        if 'bioDescription' in binding and not artist_data['bio_text']:
            artist_data['bio_text'] = binding['bioDescription']['value']
    
    # Convert empty lists to None, join education list
    for wikidata_id, data in artists_data.items():
        data['movements'] = data['movements'] if data['movements'] else None
        data['education'] = ', '.join(data['education']) if data['education'] else None
        data['influenced_by'] = data['influenced_by'] if data['influenced_by'] else None
    
    return artists_data


def enrich_artists_batch(artists_df, batch_size=50):
    # Enrich artists using batch SPARQL queries

    print(f"BATCH ENRICHMENT: {len(artists_df):,} artists in batches of {batch_size}")
    
    enriched_data = []
    # start_time = time.time()
    
    # Calculate number of batches
    total_batches = (len(artists_df) - 1) // batch_size + 1
    
    print(f"Total batches to process: {total_batches}")
    # print(f"Estimated time: ~{total_batches * 1.2 / 60:.1f} minutes\n")
    
    # successful_fetches = 0
    # failed_fetches = 0
    # no_data_count = 0
    
    # Process in batches
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(artists_df))
        
        batch = artists_df.iloc[start_idx:end_idx]
        
        print(f"Batch {batch_num + 1}/{total_batches} - Processing artists {start_idx+1} to {end_idx}")
        
        # Extract IDs for this batch
        batch_ids = batch['wiki_qid'].tolist()
        
        # Fetch batch data from Wikidata
        response = fetch_wikidata_batch(batch_ids)
        
        if response:
            # Parse the batch response
            batch_data = parse_batch_response(response, batch_ids)
            
            # Create records for each artist in batch
            for _, row in batch.iterrows():
                artist_name = row['display_name']
                wikidata_id = row['wiki_qid']
                
                # Get this artist's data from the batch response
                artist_info = batch_data.get(wikidata_id, {})
                
                # Check for any meaningful data
                has_data = any([
                    artist_info.get('movements'),
                    artist_info.get('bio_text'),
                    artist_info.get('birth_place'),
                    artist_info.get('education'),
                    artist_info.get('influenced_by')
                ])
                
                if has_data:
                    fetch_status = 'success'
                    # successful_fetches += 1
                else:
                    fetch_status = 'no_data'
                    # no_data_count += 1
                
                enriched_data.append({
                    'artist_name': artist_name,
                    'wikidata_id': wikidata_id,
                    'movements': artist_info.get('movements'),
                    'bio_text': artist_info.get('bio_text'),
                    'birth_place': artist_info.get('birth_place'),
                    'birth_place_country': artist_info.get('birth_place_country'),
                    'death_place': artist_info.get('death_place'),
                    'education': artist_info.get('education'),
                    'influenced_by': artist_info.get('influenced_by'),
                    'api_response_json': json.dumps(response),
                    'fetch_status': fetch_status
                })
            
            print(f"  ✓ Enriched {len(batch)} artists")
        else:
            # Handle failed batch - add empty records
            for _, row in batch.iterrows():
                enriched_data.append({
                    'artist_name': row['display_name'],
                    'wikidata_id': row['wiki_qid'],
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
                # failed_fetches += 1
            
            print(f"  ❌ Batch failed - marked {len(batch)} artists as failed")
        
        time.sleep(1) # Rate limiting - pauses execution for 1 seconds
        
        # Progress update every 10 batches
        if batch_num > 0 and (batch_num + 1) % 10 == 0:
            # elapsed = (time.time() - start_time) / 60
            progress = ((batch_num + 1) / total_batches) * 100
            # avg_time_per_batch = elapsed / (batch_num + 1)
            
            print(f"\n  📊 Progress Update:")
            print(f"     {progress:.1f}% complete")
            # print(f"     {elapsed:.1f}m elapsed, ~{est_remaining:.1f}m remaining")
            # print(f"     Success: {successful_fetches}, No data: {no_data_count}, Failed: {failed_fetches}\n")
    
    df = pd.DataFrame(enriched_data)
    # elapsed = (time.time() - start_time) / 60
    
    print(f"✓ Enriched {len(df):,} artist records")
    # print(f"✓ Made {total_batches} API requests (vs {len(df):,} with sequential approach)")
    # print(f"\nResults breakdown:")
    # print(f"  Success: {successful_fetches:,} ({successful_fetches/len(df)*100:.1f}%)")
    # print(f"  No data: {no_data_count:,} ({no_data_count/len(df)*100:.1f}%)")
    # print(f"  Failed: {failed_fetches:,} ({failed_fetches/len(df)*100:.1f}%)")
    
    return df


def load_to_database(df, engine):
    # Load Wikidata enrichment data to staging table with optimized batch insert
    print("LOADING TO DATABASE")
    
    # Clear existing data
    print("Truncating existing data...")
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE staging.staging_wikidata_artists"))
        conn.commit()
    print("✓ Table truncated")
    
    # Load data with batch insert
    print(f"Inserting {len(df):,} records...")
    df.to_sql(
        name='staging_wikidata_artists',
        schema='staging',
        con=engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=500  # Batch insert 500 rows at a time
    )
    
    print(f"✓ Loaded {len(df)} Wikidata enrichment records")


def verify_load(engine):
    # Verify data was loaded correctly
    print("Verifying Wikidata enrichment load...")
    
    queries = {
        'Total artists enriched': 'SELECT COUNT(*) as count FROM staging.staging_wikidata_artists',
        
        'Enrichment success rate': '''
            SELECT 
                fetch_status,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM staging.staging_wikidata_artists
            GROUP BY fetch_status
            ORDER BY count DESC
        ''',
        
        'Artists with movements (sample)': '''
            SELECT 
                artist_name,
                movements,
                birth_place,
                birth_place_country
            FROM staging.staging_wikidata_artists
            WHERE movements IS NOT NULL
            LIMIT 10
        ''',
        
        'Artists with influences (sample)': '''
            SELECT 
                artist_name,
                influenced_by
            FROM staging.staging_wikidata_artists
            WHERE influenced_by IS NOT NULL
            LIMIT 10
        ''',
        
        'Data completeness': '''
            SELECT
                COUNT(*) as total,
                COUNT(movements) as has_movements,
                COUNT(bio_text) as has_bio,
                COUNT(birth_place) as has_birth_place,
                COUNT(education) as has_education,
                COUNT(influenced_by) as has_influences,
                ROUND(COUNT(movements) * 100.0 / COUNT(*), 1) as pct_movements,
                ROUND(COUNT(bio_text) * 100.0 / COUNT(*), 1) as pct_bio,
                ROUND(COUNT(birth_place) * 100.0 / COUNT(*), 1) as pct_birth_place
            FROM staging.staging_wikidata_artists
        '''
    }
    
    with engine.connect() as conn:
        for description, query in queries.items():
            print(f"\n{description}:")
            print("-" * 60)
            result = pd.read_sql(query, conn)
            print(result.to_string(index=False))


def main():
    print("="*60)
    print("WIKIDATA ARTIST ENRICHMENT LOADER - BATCH VERSION")
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
        
        # Enrich with Wikidata using batch queries
        enriched_df = enrich_artists_batch(artists_df, batch_size=50)
        
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
    enriched_df = enrich_artists_batch(artists_df, batch_size=50)
    load_to_database(enriched_df, engine)
    return len(enriched_df)


if __name__ == "__main__":
    main()