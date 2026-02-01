"""
Load Geographic Reference Data
Uses REST Countries API for country mapping
"""

import pandas as pd
import requests ## module that allows to send HTTP requests
from sqlalchemy import create_engine, text
from datetime import datetime
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DB_CONNECTION_STRING

def create_db_connection():
# Create database connection using SQLAlchemy with connection string from config.py
    return create_engine(DB_CONNECTION_STRING)

def fetch_country_data():
    # Fetch country data from REST Countries API
    print("Fetching country data from REST Countries API...")
    
    # Specifying particular fields (as required by the API)
    fields = [
        "name", "cca2", "cca3", "region",
        "subregion", "continents", "latlng"
    ]
    fields_param = ",".join(fields)
    url = f"https://restcountries.com/v3.1/all?fields={fields_param}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        countries = response.json()
        print(f"✓ Fetched {len(countries)} countries (fields: {fields_param})")
        return countries
    except requests.exceptions.RequestException as e: #catches only the exceptions that come from HTTP/network operations
        print(f"❌ Error fetching data from REST Countries API: {e}")
        return None

def process_country_data(countries):
    # Transform API response into structured DataFrame
    print("\nProcessing country data...")
    
    data = []
    
    for country in countries:
        try:
            # Extract data with safe defaults ("{}"", "''") if empty
            # name - dictionnary with array of values(official ctry name, native, etc.)
            country_name = country.get('name', {}).get('common', '')
            
            # ISO codes (ex. cca2: 'FR', cca3: 'FRA')
            iso2 = country.get('cca2', '')
            iso3 = country.get('cca3', '')
            
            # Geographic info
            continent = country.get('continents', [''])[0] if country.get('continents') else ''
            region = country.get('region', '')
            subregion = country.get('subregion', '')
            
            # Coordinates
            latlng = country.get('latlng', [None, None])
            latitude = latlng[0] if len(latlng) > 0 else None
            longitude = latlng[1] if len(latlng) > 1 else None
            
            data.append({
                'country_name': country_name,
                'country_code_iso2': iso2,
                'country_code_iso3': iso3,
                'continent': continent,
                'region': region,
                'subregion': subregion,
                'latitude': latitude,
                'longitude': longitude,
                'source': 'REST Countries API v3.1'
            })
        except Exception as e:
            print(f"  Warning: Error processing {country.get('name', {}).get('common', 'unknown')}: {e}")
            continue
    
    df = pd.DataFrame(data)
    print(f"✓ Processed {len(df)} country records")

    # Ensure country_name exists, is not null
    df = df[df['country_name'].notna() & (df['country_name'] != "")]
    # Ensure lowercase normalization
    df['country_name'] = df['country_name'].str.strip()
    return df

def country_to_nationality(country):
    # Generating nationality adjectives for each country name
    special_cases = {
        "Czechia": "czech",
        "United States": "american",
        "United Kingdom": "british",
        "Netherlands": "dutch",
        "Switzerland": "swiss",
        "Germany": "german",
        "France": "french",
        "Italy": "italian",
        "Spain": "spanish",
        "Portugal": "portuguese",
        "Poland": "polish",
        "China": "chinese",
        "Japan": "japanese",
        "Vietnam": "vietnamese",
        "Thailand": "thai",
        "Greece": "greek",
        "Sweden": "swedish",
        "Norway": "norwegian",
        "Denmark": "danish",
        "Finland": "finnish",
        "Iceland": "icelandic",
        "Ireland": "irish",
        "Belgium": "belgian",
        "Hungary": "hungarian",
        "Slovakia": "slovak",
        "Ukraine": "ukrainian",
        "Belarus": "belarusian",
        "Turkey": "turkish",
        "Pakistan": "pakistani",
        "Bangladesh": "bangladeshi",
        "Iran": "iranian",
        "Iraq": "iraqi",
        "Israel": "israeli",
        "Egypt": "egyptian",
        "Morocco": "moroccan",
        "Kenya": "kenyan",
        "Mexico": "mexican",
        "Brazil": "brazilian",
        "Argentina": "argentine",
        "Chile": "chilean",
        "Peru": "peruvian",
        "New Zealand": "new zealander",
        "Kazakhstan": "kazakh",
        "Azerbaijan": "azerbaijani",
        "Canada": "canadian",
        "South Korea": "korean",
        "Uruguay": "uruguayan",
        "Scotland": "scottish",
        "Haiti": "haitian",
        "Puerto Rico": "puerto rican",
        "Canada (Inuit regions)": "canadian inuit",
        "Lebanon": "lebanese",
        "Zimbabwe": "zimbabwean",
        "Democratic Republic of the Congo": "congolese",
        "Bosnia and Herzegovina": "bosnian",
        "Palestine": "palestinian",
        "Senegal": "senegalese",
        "Czechoslovakia": "czechoslovakian",
        "Taiwan": "taiwanese",
        "Niger": "nigerien",
        "Philippines": "filipino",
        "North Macedonia": "macedonian",
        "Panama": "panamanian",
        "Wales": "welsh",
        "Sudan": "sudanese",
        "Luxembourg": "luxembourger",
        "Mali": "malian",
        "Singapore": "singaporean",
        "Ghana": "ghanaian",
        "El Salvador": "salvadoran",
        "Paraguay": "paraguayan",
        "Catalonia": "catalan",
        "Bahamas": "bahamian",
        "Mozambique": "mozambican",
        "England": "english",
        "Trinidad and Tobago": "trinidad and tobagonian",
        "Azerbaijan": "azerbaijani",
        "Kyrgyzstan": "kyrgyz",
        "Afghanistan": "afghan",
        "United Arab Emirates": "emirati"
    }

    if country in special_cases:
        return special_cases[country]
    else:
        # Default heuristic: add "-ian" or "-ese" for Asian countries ending in 'a'
        if country.endswith("a"):
            return country[:-1].lower() + "an"
        else:
            return country.lower() + "ian"
        
def add_custom_mappings(df):
    # Add custom mappings for adjusting data
    print("\nAdding custom nationality mappings...")
    
    custom_mappings = [
        # For Nationality Unknown case    
        {'country_name': 'Nationality unknown', 'country_code_iso2': 'XX', 'country_code_iso3': 'XXX',
         'continent': 'Unknown', 'region': 'Unknown', 'subregion': 'Unknown',
         'latitude': None, 'longitude': None, 'source': 'Custom mapping'},
    ]
    
    custom_df = pd.DataFrame(custom_mappings)
    # Adding custom dataframe values to the original one for better mapping
    df = pd.concat([df, custom_df], ignore_index=True) # ignore_index=True resets row numbers (0, 1, 2, ... instead of 0, 1, ..., 250, 0, 1, ...)
    
    print(f"✓ Added {len(custom_mappings)} custom mappings")
    return df

def load_to_database(df, engine):
    # Load geographic data to staging table
    print("\nLoading data to PostgreSQL...")
    
    # Clear existing data
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE staging.staging_geographic_reference"))
        conn.commit()
    
    # Load to database
    df.to_sql(
        name='staging_geographic_reference',
        schema='staging',
        con=engine,
        if_exists='append', # append to existing table
        index=False,
        method='multi', # faster batch insert
        chunksize=100
    )
    
    print(f"✓ Loaded {len(df)} geographic records")

# Verify data was loaded correctly
def verify_load(engine):
    print("\n" + "="*60)
    print("Verifying geographic data load...")
    print("="*60)
    
    queries = {
        'Total countries': 'SELECT COUNT(*) FROM staging.staging_geographic_reference',
        'Countries by continent': '''
            SELECT continent, COUNT(*) as count 
            FROM staging.staging_geographic_reference 
            GROUP BY continent 
            ORDER BY count DESC
        ''',
        'Sample countries': '''
            SELECT country_name, country_code_iso2, continent, region, nationality
            FROM staging.staging_geographic_reference 
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
    print("GEOGRAPHIC REFERENCE DATA LOADER")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        # Fetch data from API
        countries = fetch_country_data()
        if not countries:
            return
        
        # Process data
        df = process_country_data(countries)
        
        # Add custom mappings
        df = add_custom_mappings(df)

        # mapping coutry to nationality
        df["nationality"] = df["country_name"].apply(country_to_nationality)

        # Remove duplicates before loading (to satisfy unique constraint)
        before = len(df)
        df = df.drop_duplicates(subset=['country_code_iso2'], keep='first')
        after = len(df)
        print(f"✓ Removed {before - after} duplicate records based on ISO2 code")
        print(f"✓ {after} records remaining before DB load")
        
        # Connect to database
        print("\nConnecting to PostgreSQL...")
        engine = create_db_connection()
        print("✓ Connected successfully")
        
        # Load to database
        load_to_database(df, engine)
        
        # Verify
        verify_load(engine)
        
        print("\n" + "="*60)
        print("LOAD COMPLETE!")
        print("="*60)
        print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()

def fetch_and_load_geographic_data():
    # Airflow-callable function
    countries = fetch_country_data()
    df = process_country_data(countries)
    df = add_custom_mappings(df)
    df["nationality"] = df["country_name"].apply(country_to_nationality)
    df = df.drop_duplicates(subset=['country_code_iso2'], keep='first')
    engine = create_db_connection()
    load_to_database(df, engine)
    return len(df)

if __name__ == "__main__":
    main()