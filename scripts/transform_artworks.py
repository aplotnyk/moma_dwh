"""
Transform Artworks Data
Cleans, standardizes artwork data from staging
"""

import pandas as pd
import numpy as np # for work with multi-dimensional arrays(may use it in future)
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import os
import re
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

def load_staging_artworks(engine):
    # Load artworks from staging
    print("Loading artworks from staging...")
    
    query = """
        SELECT 
            object_id,
            title,
            date,
            medium,
            dimensions,
            credit_line,
            accession_number,
            classification,
            department,
            date_acquired,
            cataloged,
            url,
            image_url,
            height_cm,
            width_cm
        FROM staging.staging_moma_artworks
        WHERE object_id IS NOT NULL
    """
    
    df = pd.read_sql(query, engine)
    print(f"✓ Loaded {len(df):,} artworks from staging")
    return df

def parse_creation_date(date_str):
    # Extract year and certainty from date string
    if pd.isna(date_str) or date_str == 'unknown' or date_str == 'Unknown' or date_str == 'no date' or date_str == 'n.d.' or date_str == '':
        return None, None, 'unknown'
    
    date_str = str(date_str).strip()# removes whitespaces
    
    # Pattern: c. 1950 
    if date_str.startswith('c.') or date_str.startswith('ca.') or date_str.startswith('published') or date_str.startswith('printed'):
        try:
            year = int(re.search(r'\d{4}', date_str).group())
            return year, date_str, 'circa'
        except:
            return None, date_str, 'unknown'

    # Pattern: early 1950, November 1950     
    if date_str.startswith('early')  or date_str.startswith('late') or date_str.startswith('Summer') or date_str.startswith('November') or date_str.startswith('May') or date_str.startswith('March') or date_str.startswith('June') or date_str.startswith('January') or date_str.startswith('February') or date_str.startswith('December') or date_str.startswith('August') or date_str.startswith('April') or date_str.startswith('October') or date_str.startswith('September'):
        try:
            year = int(re.search(r'\d{4}', date_str).group())
            return year, date_str, 'exact'
        except:
            return None, date_str, 'unknown'    
    
    # Pattern: 1920-1925 (range)
    if '-' in date_str and len(date_str) <= 10:
        try:
            years = re.findall(r'\d{4}', date_str)
            if len(years) == 2:
                # Take midpoint of range
                year = (int(years[0]) + int(years[1])) // 2
                return year, date_str, 'range'
        except:
            pass
    
    # Pattern: exact year
    try:
        year = int(re.search(r'\d{4}', date_str).group())
        if 1000 <= year <= 2024:
            return year, date_str, 'exact'
    except:
        pass
    
    return None, date_str, 'unknown'

def parse_dimensions(dim_str, height_str, width_str):
    # Extract dimensions, try individual columns first
    height = parse_single_dimension(height_str)
    width = parse_single_dimension(width_str)
    
    # If still None, try parsing from dimensions string
    if height is None or width is None:
        parsed = parse_dimension_string(dim_str)
        if parsed:
            height = height or parsed.get('height')
            width = width or parsed.get('width')
    
    return height, width

def parse_single_dimension(dim_str):
    # Parse single dimension value
    if pd.isna(dim_str) or dim_str == 'None' or dim_str == '':
        return None
    
    try:
        return float(dim_str)
    except:
        return None

def parse_dimension_string(dim_str):
    # Parse dimension string, with cm measurements
    # Last number before 'cm' = width, previous = height
    if pd.isna(dim_str) or dim_str == 'not found' or dim_str == '':
        return None
    
    try:
        dim_str = str(dim_str)
        
        if 'cm' in dim_str.lower():
            # Extract everything before 'cm' 
            cm_match = re.search(r'(.+?)\s*cm', dim_str, re.IGNORECASE)# one or more chars (.+?) + optional spaces \s
            if cm_match:
                cm_part = cm_match.group(1)# extract the before 'cm' part (.+?) to isolate from the rest of the string we don't need
                
                # Find all numbers (integers and decimals)
                numbers = re.findall(r'\d+\.?\d*', cm_part)
                
                if len(numbers) >= 2:
                    # Last number before 'cm' = width
                    # Second to last = height
                    width = float(numbers[-1])
                    height = float(numbers[-2])
                    
                    return {
                        'height': height,
                        'width': width,
                    }
        
        # Fallback: if no 'cm' found, try to parse first 2 numbers
        numbers = re.findall(r'\d+\.?\d*', dim_str)
        if len(numbers) >= 2:
            return {
                'height': float(numbers[0]),
                'width': float(numbers[1]),
            }
            
    except Exception as e:
        pass
    
    return None


def parse_acquisition_date(date_str):
    # Convert acquisition date to proper date format
    if pd.isna(date_str) or date_str == '':
        return None, None
    
    try:
        date_obj = pd.to_datetime(date_str)
        return date_obj, date_obj.year
    except:
        return None, None

def extract_acquisition_method(credit_line):
# Extract acquisition method from credit line"""
    if pd.isna(credit_line) or credit_line == 'None':
        return 'Unknown'
    
    credit_lower = str(credit_line).lower()
    
    if 'gift' in credit_lower or 'louis e. stern' in credit_lower or 'fund for the twenty-first century' in credit_lower or 'given' in credit_lower:
        return 'Gift'
    elif 'purchase' in credit_lower or 'purchased' in credit_lower:
        return 'Purchase'
    elif 'bequest' in credit_lower:
        return 'Bequest'
    elif 'exchange' in credit_lower:
        return 'Exchange'
    else:
        return 'Unknown'

def calculate_artwork_quality_score(row):
    #Calculate data quality score for artwork
    score = 0.0
    
    # Title (15%)
    if pd.notna(row['title']) and len(str(row['title'])) > 0:
        score += 0.15
    
    # Creation date (25%)
    if pd.notna(row['creation_date_raw']) and str(row['creation_date_raw']) != '':
        score += 0.25
    
    # Acquisition date (15%)
    if pd.notna(row['date_acquired']) and str(row['date_acquired']) != '':
        score += 0.15
    
    # Dimensions (20%) - both width and height must be present
    if pd.notna(row['width_cm']) and pd.notna(row['height_cm']):
        score += 0.20
    
    # Classification (15%)
    if pd.notna(row['classification'])and row['classification'] != '(not assigned))':
        score += 0.15
    
    # Cataloged (10%)
    if row['cataloged']:
        score += 0.10
    
    return round(score, 2)

def transform_artworks(df_artworks):
    # Apply all transformations to artwork data
    print("\nTransforming artwork data...")
    
    # 1. Parse creation dates
    print("  Parsing creation dates...")
    date_parsed = df_artworks['date'].apply(parse_creation_date)
    df_artworks['creation_year'] = date_parsed.apply(lambda x: x[0])
    df_artworks['creation_date_raw'] = date_parsed.apply(lambda x: x[1])
    df_artworks['date_certainty'] = date_parsed.apply(lambda x: x[2])
    
    # 2. Parse dimensions
    print("  Parsing dimensions...")
    dimensions_parsed = df_artworks.apply(
        lambda row: parse_dimensions(
            row['dimensions'], 
            row['height_cm'], 
            row['width_cm'],
        ), 
        axis=1 # apply to each row
    )
    df_artworks['height_cm'] = dimensions_parsed.apply(lambda x: x[0])
    df_artworks['width_cm'] = dimensions_parsed.apply(lambda x: x[1])
    df_artworks['dimensions_text'] = df_artworks['dimensions']
    
    # 3. Parse acquisition date
    print("  Parsing acquisition dates...")
    acq_parsed = df_artworks['date_acquired'].apply(parse_acquisition_date)
    df_artworks['date_acquired'] = acq_parsed.apply(lambda x: x[0])
    df_artworks['acquisition_year'] = acq_parsed.apply(lambda x: x[1])
    
    # 4. Extract acquisition method
    print("  Extracting acquisition method...")
    df_artworks['acquisition_method'] = df_artworks['credit_line'].apply(extract_acquisition_method)
    
    # 5. Convert boolean fields
    print("  Converting boolean fields...")
    df_artworks['cataloged'] = df_artworks['cataloged'].apply(
        lambda x: True if str(x).upper() in ['Y', 'YES', 'TRUE', '1'] else False
    )
    
    # 6. Calculate quality score
    df_artworks['data_quality_score'] = df_artworks.apply(calculate_artwork_quality_score, axis=1) # apply to each row
    
    # 7. Rename columns
    df_artworks['moma_url'] = df_artworks['url']
    df_artworks['artwork_id'] = df_artworks['object_id']
    
    print(f"✓ Transformed {len(df_artworks):,} artworks")
    return df_artworks

def load_to_transformed(df, engine):
    #Load transformed artworks to database
    print("\nLoading to transformed schema...")
    
    columns_to_load = [
        'artwork_id',
        'title',
        'creation_year',
        'creation_date_raw',
        'date_certainty',
        'medium',
        'classification',
        'department',
        'height_cm',
        'width_cm',
        'dimensions_text',
        'date_acquired',
        'acquisition_year',
        'credit_line',
        'acquisition_method',
        'accession_number',
        'cataloged',
        'moma_url',
        'image_url',
        'data_quality_score'
    ]
    
    df_load = df[columns_to_load].copy()
    
    # Clear existing data
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE transformed.transformed_artworks CASCADE"))
        conn.commit()
    
    # Load data
    df_load.to_sql(
        name='transformed_artworks',
        schema='transformed',
        con=engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    print(f"✓ Loaded {len(df_load):,} transformed artworks")

def verify_transformation(engine):
    # Verify transformed artwork data
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
    
    with engine.connect() as conn:
        for description, query in queries.items():
            print(f"\n{description}:")
            result = pd.read_sql(query, conn)
            print(result.to_string(index=False))

def main():
    print("="*60)
    print("ARTWORK DATA TRANSFORMATION")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        # Connect to database
        engine = create_db_connection()
        print("✓ Connected to database\n")
        
        # Load staging data
        df_artworks = load_staging_artworks(engine)
        
        # Transform
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
    # Airflow-callable function
    engine = create_db_connection()
    df_artworks = load_staging_artworks(engine)
    df_transformed = transform_artworks(df_artworks)
    load_to_transformed(df_transformed, engine)
    return len(df_transformed)

if __name__ == "__main__":
    main()