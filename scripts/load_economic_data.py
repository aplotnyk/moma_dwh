"""
Load Economic Context Data
Fetches historical economic data from FRED API
"""

import pandas as pd
import requests
from sqlalchemy import create_engine, text
from datetime import datetime
import os
import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DB_CONNECTION_STRING, FRED_API_KEY

def create_db_connection():
# Create database connection using SQLAlchemy with connection string from config.py
    return create_engine(DB_CONNECTION_STRING)

# Fetch a single time series from FRED API
def fetch_fred_series(series_id, series_name):
    print(f"  Fetching {series_name}...")
    
    url = f"https://api.stlouisfed.org/fred/series/observations"
    params = {
        'series_id': series_id,
        'api_key': FRED_API_KEY,
        'file_type': 'json',
        'observation_start': '1929-01-01',  # MoMA founding year
        'observation_end': '2024-12-31',
        'frequency': 'a',  # annual data
        'aggregation_method': 'avg'
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status() # Prevents program from silently continuing in case of error
        data = response.json()
        
        if 'observations' in data:
            observations = data['observations']
            df = pd.DataFrame(observations) # Converts from list dictionnary to DataFrame(table with rows&columns)
            df['year'] = pd.to_datetime(df['date']).dt.year # Adds 'year' column to DataFrame
            df['value'] = pd.to_numeric(df['value'], errors='coerce') # 'coerce' errors param sets invalid parsing as NaN without raising an error
            df = df[['year', 'value']].dropna() # df[['col']] returns DataFrame vs df['col'] (Dimensional series); .dropna() removes rows with missing(NaN) values
            print(f"    ✓ Fetched {len(df)} observations")
            return df
        else:
            print(f"    ⚠️  No data returned")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"    ❌ Error: {e}")
        return pd.DataFrame()

# Fetch all economic indicators from FRED
def fetch_all_economic_data():
    print("\nFetching economic data from FRED API...")
    print("(This will take 2-3 minutes due to API rate limits)")
    
    # FRED series IDs
    series_to_fetch = {
        'GDP': 'GDP',                    # GDP in billions
        'CPIAUCSL': 'CPI',              # Consumer Price Index
        'UNRATE': 'Unemployment',        # Unemployment Rate
        'SP500': 'SP500'                 # S&P 500 Index
    }
    
    dataframes = {}
    
    for series_id, name in series_to_fetch.items():
        df = fetch_fred_series(series_id, name)
        if not df.empty:
            dataframes[name] = df
        time.sleep(1)  # Rate limiting - pauses execution for 1 second
    
    return dataframes

# Merge all economic indicators into single DataFrame
def merge_economic_data(dataframes):
    print("\nMerging economic indicators...")
    
    years = pd.DataFrame({'year': range(1929, 2025)}) # Create base DataFrame col.years, val. 1929-2024
    
    # Merge each indicator
    for name, df in dataframes.items():
        df = df.rename(columns={'value': name})
        years = years.merge(df[['year', name]], on='year', how='left') # Keep all years from left df(years), if value empty - 'NaN'
    
    # Rename columns to match schema
    years = years.rename(columns={
        'GDP': 'gdp_usd_billions',
        'Unemployment': 'unemployment_rate_percent',
        'SP500': 'sp500_close'
    })
    
    # Calculate inflation rate from CPI
    if 'CPI' in years.columns:
        years['inflation_rate_percent'] = years['CPI'].pct_change() * 100
        years = years.drop('CPI', axis=1)# drop CPI cp;umn since we only need infl.rate
    
    print(f"✓ Merged data for {len(years)} years")
    return years

# Add economic period classifications
def add_economic_periods(df):
    print("\nClassifying economic periods...")
    
    # Major recessions and their periods
    recessions = [
        (1929, 1939, 'Great Depression'),
        (1945, 1945, 'Post-WWII Recession'),
        (1949, 1949, 'Recession of 1949'),
        (1953, 1954, 'Recession of 1953'),
        (1957, 1958, 'Recession of 1958'),
        (1960, 1961, 'Recession of 1960-61'),
        (1969, 1970, 'Recession of 1969-70'),
        (1973, 1975, 'Oil Crisis Recession'),
        (1980, 1982, 'Early 1980s Recession'),
        (1990, 1991, 'Early 1990s Recession'),
        (2001, 2001, 'Dot-com Recession'),
        (2007, 2009, 'Great Recession'),
        (2020, 2020, 'COVID-19 Recession')
    ]
    
    df['economic_period'] = 'Expansion' # Default value
    
    for start, end, _ in recessions:
        mask = (df['year'] >= start) & (df['year'] <= end)
        df.loc[mask, 'economic_period'] = 'Recession' # Recession value for periods in question
    
    print("✓ Economic periods classified")
    return df

# Add major historical/economic events
def add_major_events(df):
    print("\nAdding major historical events...")
    
    events = {
        1929: 'Stock Market Crash, Great Depression begins',
        1933: 'New Deal programs begin',
        1939: 'World War II begins',
        1941: 'US enters World War II',
        1945: 'World War II ends',
        1950: 'Korean War begins',
        1953: 'Korean War ends',
        1964: 'Vietnam War escalates',
        1973: 'Oil Crisis, OPEC embargo',
        1975: 'Vietnam War ends',
        1987: 'Black Monday stock market crash',
        1991: 'Gulf War',
        2001: 'September 11 attacks, dot-com bubble burst',
        2003: 'Iraq War begins',
        2007: 'Subprime mortgage crisis begins',
        2008: 'Financial crisis, Lehman Brothers collapse',
        2010: 'European debt crisis',
        2020: 'COVID-19 pandemic begins',
        2022: 'russia-Ukraine war, high inflation'
    }
    
    df['major_events'] = df['year'].map(events)
    
    print(f"✓ Added {len(events)} major event markers")
    return df

# Load economic data to staging table
def load_to_database(df, engine):
    print("\nLoading data to PostgreSQL...")
    
    # Add metadata
    df['source'] = 'Federal Reserve Economic Data (FRED)'
    
    # Clear existing data
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE staging.staging_economic_data"))
        conn.commit()
    
    # Load data
    df.to_sql(
        name='staging_economic_data',
        schema='staging',
        con=engine,
        if_exists='append', # append to existing table
        index=False,
        method='multi' # faster batch insert
    )
    
    print(f"✓ Loaded {len(df)} economic records")

# Verify data was loaded correctly
def verify_load(engine):
    print("\n" + "="*60)
    print("Verifying economic data load...")
    print("="*60)
    
    queries = {
        'Total years': 'SELECT COUNT(*) FROM staging.staging_economic_data',
        'Data completeness': '''
            SELECT 
                COUNT(*) as total_years,
                COUNT(gdp_usd_billions) as has_gdp,
                COUNT(inflation_rate_percent) as has_inflation,
                COUNT(unemployment_rate_percent) as has_unemployment,
                COUNT(sp500_close) as has_sp500
            FROM staging.staging_economic_data
        ''',
        'Recessions': '''
            SELECT year, economic_period, major_events
            FROM staging.staging_economic_data
            WHERE economic_period = 'Recession'
            ORDER BY year DESC
            LIMIT 10
        ''',
        'Sample data': '''
            SELECT year, gdp_usd_billions, inflation_rate_percent, economic_period
            FROM staging.staging_economic_data
            WHERE year IN (1929, 1950, 1975, 2000, 2020, 2024)
            ORDER BY year
        '''
    }
    
    with engine.connect() as conn:
        for description, query in queries.items():
            print(f"\n{description}:")
            result = pd.read_sql(query, conn)
            print(result.to_string(index=False))

def main():
    print("="*60)
    print("ECONOMIC CONTEXT DATA LOADER")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Check API key
    if not FRED_API_KEY:
        print("❌ ERROR: FRED_API_KEY not found in .env file")
        print("\nPlease:")
        print("1. Get free API key from: https://fred.stlouisfed.org/")
        print("2. Add to .env file: FRED_API_KEY=your_key_here")
        return
    
    try:
        # Fetch data from FRED
        dataframes = fetch_all_economic_data()
        
        if not dataframes:
            print("❌ No data fetched. Check your API key.")
            return
        
        # Merge all indicators
        df = merge_economic_data(dataframes)
        
        # Add classifications
        df = add_economic_periods(df)
        df = add_major_events(df)
        
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

def fetch_and_load_economic_data():
    # Airflow-callable function
    dataframes = fetch_all_economic_data()
    df = merge_economic_data(dataframes)
    df = add_economic_periods(df)
    df = add_major_events(df)
    engine = create_db_connection()
    load_to_database(df, engine)
    return len(df)

if __name__ == "__main__":
    main()