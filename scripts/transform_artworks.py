"""
Transform Artworks Data - Vectorized & Optimized
Uses vectorized operations instead of .apply() for 10-20x speedup
Loads all data at once (SQLAlchemy upgrade fixed compatibility)
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import os
import re
from pathlib import Path
import sys

load_dotenv()
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DB_CONNECTION_STRING

def create_db_connection():
    return create_engine(DB_CONNECTION_STRING)

def load_staging_artworks(engine):
    print("Loading artworks from staging...")
    
    query = """
        SELECT 
            object_id, title, date, medium, dimensions,
            credit_line, accession_number, classification,
            department, date_acquired, cataloged,
            url, image_url, height_cm, width_cm
        FROM staging.staging_moma_artworks
        WHERE object_id IS NOT NULL
    """
    
    df = pd.read_sql_query(query, engine)
    print(f"✓ Loaded {len(df):,} artworks from staging")
    return df

def parse_creation_dates_vectorized(df):
    """Vectorized date parsing - 10x faster than apply"""
    print("  Parsing creation dates (vectorized)...")
    
    date_series = df['date'].fillna('').astype(str).str.strip()
    
    # Initialize result arrays
    years = pd.Series([None] * len(df), dtype='Int64')
    certainties = pd.Series(['unknown'] * len(df))
    
    # Pattern 1: c., ca., published, printed (circa)
    circa_mask = date_series.str.startswith(('c.', 'ca.', 'published', 'printed'))
    circa_extract = date_series[circa_mask].str.extract(r'(\d{4})', expand=False)
    years.loc[circa_mask] = pd.to_numeric(circa_extract, errors='coerce')
    certainties.loc[circa_mask] = 'circa'
    
    # Pattern 2: early, late, months (exact)
    month_prefixes = ('early', 'late', 'Summer', 'November', 'May', 'March', 'June', 
                      'January', 'February', 'December', 'August', 'April', 'October', 'September')
    exact_mask = date_series.str.startswith(month_prefixes) & years.isna()
    exact_extract = date_series[exact_mask].str.extract(r'(\d{4})', expand=False)
    years.loc[exact_mask] = pd.to_numeric(exact_extract, errors='coerce')
    certainties.loc[exact_mask] = 'exact'
    
    # Pattern 3: 1920-1925 (range) - take midpoint
    range_mask = date_series.str.contains('-', na=False) & (date_series.str.len() <= 10) & years.isna()
    if range_mask.any():
        range_data = date_series[range_mask].str.extractall(r'(\d{4})')[0]
        if not range_data.empty:
            grouped = range_data.groupby(level=0)
            for idx, group in grouped:
                if len(group) == 2:
                    midpoint = (int(group.iloc[0]) + int(group.iloc[1])) // 2
                    years.loc[idx] = midpoint
                    certainties.loc[idx] = 'range'
    
    # Pattern 4: exact year (remaining)
    remaining_mask = years.isna() & (date_series != '')
    remaining_extract = date_series[remaining_mask].str.extract(r'(\d{4})', expand=False)
    remaining_years = pd.to_numeric(remaining_extract, errors='coerce')
    valid_mask = remaining_mask & (remaining_years >= 1000) & (remaining_years <= 2024)
    years.loc[valid_mask] = remaining_years[valid_mask]
    certainties.loc[valid_mask] = 'exact'
    
    df['creation_year'] = years
    df['creation_date_raw'] = df['date']
    df['date_certainty'] = certainties
    
    return df

def parse_dimensions_vectorized(df):
    """Vectorized dimension parsing"""
    print("  Parsing dimensions (vectorized)...")
    
    # Parse individual columns
    df['height_cm'] = pd.to_numeric(df['height_cm'], errors='coerce')
    df['width_cm'] = pd.to_numeric(df['width_cm'], errors='coerce')
    
    # For missing values, try parsing from dimensions string
    missing_mask = df['height_cm'].isna() | df['width_cm'].isna()
    
    if missing_mask.any():
        dim_str = df.loc[missing_mask, 'dimensions'].fillna('').astype(str)
        
        # Extract numbers before 'cm'
        cm_matches = dim_str.str.extract(r'(.+?)\s*cm', flags=re.IGNORECASE)[0]
        
        for idx in cm_matches[cm_matches.notna()].index:
            numbers = re.findall(r'\d+\.?\d*', str(cm_matches[idx]))
            if len(numbers) >= 2:
                # Last number = width, second to last = height
                if pd.isna(df.loc[idx, 'width_cm']):
                    df.loc[idx, 'width_cm'] = float(numbers[-1])
                if pd.isna(df.loc[idx, 'height_cm']):
                    df.loc[idx, 'height_cm'] = float(numbers[-2])
    
    df['dimensions_text'] = df['dimensions']
    return df

def parse_acquisition_dates_vectorized(df):
    """Vectorized acquisition date parsing"""
    print("  Parsing acquisition dates (vectorized)...")
    
    df['date_acquired'] = pd.to_datetime(df['date_acquired'], errors='coerce')
    df['acquisition_year'] = df['date_acquired'].dt.year
    
    return df

def extract_acquisition_method_vectorized(df):
    """Vectorized acquisition method extraction"""
    print("  Extracting acquisition method (vectorized)...")
    
    credit_lower = df['credit_line'].fillna('').astype(str).str.lower()
    
    # Default to Unknown
    df['acquisition_method'] = 'Unknown'
    
    # Gift (check multiple patterns)
    gift_mask = (credit_lower.str.contains('gift', na=False) | 
                 credit_lower.str.contains('louis e. stern', na=False) |
                 credit_lower.str.contains('fund for the twenty-first century', na=False) |
                 credit_lower.str.contains('given', na=False))
    df.loc[gift_mask, 'acquisition_method'] = 'Gift'
    
    # Purchase
    purchase_mask = (credit_lower.str.contains('purchase', na=False) | 
                     credit_lower.str.contains('purchased', na=False))
    df.loc[purchase_mask, 'acquisition_method'] = 'Purchase'
    
    # Bequest
    bequest_mask = credit_lower.str.contains('bequest', na=False)
    df.loc[bequest_mask, 'acquisition_method'] = 'Bequest'
    
    # Exchange
    exchange_mask = credit_lower.str.contains('exchange', na=False)
    df.loc[exchange_mask, 'acquisition_method'] = 'Exchange'
    
    return df

def calculate_quality_scores_vectorized(df):
    """Vectorized quality score calculation"""
    print("  Calculating quality scores (vectorized)...")
    
    score = pd.Series(0.0, index=df.index)
    
    # Title (15%)
    has_title = df['title'].notna() & (df['title'].astype(str).str.len() > 0)
    score += has_title * 0.15
    
    # Creation date (25%)
    has_date = df['creation_date_raw'].notna() & (df['creation_date_raw'].astype(str) != '')
    score += has_date * 0.25
    
    # Acquisition date (15%)
    has_acq = df['date_acquired'].notna() & (df['date_acquired'].astype(str) != '')
    score += has_acq * 0.15
    
    # Dimensions (20%)
    has_dims = df['width_cm'].notna() & df['height_cm'].notna()
    score += has_dims * 0.20
    
    # Classification (15%)
    has_class = df['classification'].notna() & (df['classification'] != '(not assigned)')
    score += has_class * 0.15
    
    # Cataloged (10%)
    score += df['cataloged'] * 0.10
    
    df['data_quality_score'] = score.round(2)
    
    return df

def transform_artworks(df_artworks):
    """Apply all transformations using vectorized operations"""
    print("\nTransforming artwork data...")
    
    # 1. Parse creation dates (vectorized)
    df_artworks = parse_creation_dates_vectorized(df_artworks)
    
    # 2. Parse dimensions (vectorized)
    df_artworks = parse_dimensions_vectorized(df_artworks)
    
    # 3. Parse acquisition dates (vectorized)
    df_artworks = parse_acquisition_dates_vectorized(df_artworks)
    
    # 4. Extract acquisition method (vectorized)
    df_artworks = extract_acquisition_method_vectorized(df_artworks)
    
    # 5. Convert cataloged boolean (vectorized)
    print("  Converting boolean fields (vectorized)...")
    cataloged_str = df_artworks['cataloged'].fillna('').astype(str).str.upper()
    df_artworks['cataloged'] = cataloged_str.isin(['Y', 'YES', 'TRUE', '1'])
    
    # 6. Calculate quality scores (vectorized)
    df_artworks = calculate_quality_scores_vectorized(df_artworks)
    
    # 7. Rename columns
    df_artworks['moma_url'] = df_artworks['url']
    df_artworks['artwork_id'] = df_artworks['object_id']
    
    print(f"✓ Transformed {len(df_artworks):,} artworks")
    return df_artworks

def load_to_transformed(df, engine):
    """Load transformed artworks to database"""
    print("\nLoading to transformed schema...")
    
    columns_to_load = [
        'artwork_id', 'title', 'creation_year', 'creation_date_raw',
        'date_certainty', 'medium', 'classification', 'department',
        'height_cm', 'width_cm', 'dimensions_text', 'date_acquired',
        'acquisition_year', 'credit_line', 'acquisition_method',
        'accession_number', 'cataloged', 'moma_url', 'image_url',
        'data_quality_score'
    ]
    
    df_load = df[columns_to_load].copy()
    
    # Clear existing data
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE transformed.transformed_artworks CASCADE"))
    
    # Create fresh engine for to_sql
    from sqlalchemy import create_engine as make_engine
    write_engine = make_engine(DB_CONNECTION_STRING)
    
    # Load in chunks for efficiency
    df_load.to_sql(
        name='transformed_artworks',
        schema='transformed',
        con=write_engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=5000  # Larger chunks since we're not transforming
    )
    
    write_engine.dispose()
    
    print(f"✓ Loaded {len(df_load):,} transformed artworks")

def verify_transformation(engine):
    """Verify transformed artwork data"""
    print("\n" + "="*60)
    print("Verifying artwork transformation...")
    print("="*60)
    
    queries = {
        'Total artworks': 'SELECT COUNT(*) FROM transformed.transformed_artworks',
        'Quality distribution': '''
            SELECT 
                CASE 
                    WHEN data_quality_score >= 0.8 THEN 'Excellent (0.8-1.0)'
                    WHEN data_quality_score >= 0.6 THEN 'Good (0.6-0.8)'
                    WHEN data_quality_score >= 0.4 THEN 'Fair (0.4-0.6)'
                    ELSE 'Poor (0.0-0.4)'
                END as quality_tier,
                COUNT(*) as count
            FROM transformed.transformed_artworks
            GROUP BY quality_tier
            ORDER BY quality_tier
        ''',
        'Classification categories': '''
            SELECT 
                classification,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
            FROM transformed.transformed_artworks
            GROUP BY classification
            ORDER BY count DESC
            LIMIT 10
        ''',
        'Date certainty': '''
            SELECT 
                date_certainty,
                COUNT(*) as count
            FROM transformed.transformed_artworks
            GROUP BY date_certainty
            ORDER BY count DESC
        ''',
        'Acquisition methods': '''
            SELECT 
                acquisition_method,
                COUNT(*) as count
            FROM transformed.transformed_artworks
            GROUP BY acquisition_method
            ORDER BY count DESC
        '''
    }
    
    for description, query in queries.items():
        print(f"\n{description}:")
        result = pd.read_sql_query(query, engine)
        print(result.to_string(index=False))

def main():
    print("="*60)
    print("ARTWORK DATA TRANSFORMATION")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        engine = create_db_connection()
        print("✓ Connected to database\n")
        
        # Load staging data (all at once - SQLAlchemy fixed)
        df_artworks = load_staging_artworks(engine)
        
        # Transform (vectorized operations)
        df_transformed = transform_artworks(df_artworks)
        
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

def run_transform_artworks():
    """Airflow-callable function"""
    engine = create_db_connection()
    df_artworks = load_staging_artworks(engine)
    df_transformed = transform_artworks(df_artworks)
    load_to_transformed(df_transformed, engine)
    return len(df_transformed)

if __name__ == "__main__":
    main()