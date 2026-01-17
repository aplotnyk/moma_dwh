from datetime import datetime, timedelta
import sys
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# # Load environment variables
# load_dotenv()

# # Add project root to Python path so we can import config
# sys.path.insert(0, str(Path(__file__).parent.parent))
# from config import DB_CONNECTION_STRING

# def create_db_connection():
# # Create database connection using SQLAlchemy with connection string from config.py
#     return create_engine(DB_CONNECTION_STRING)

def verify_staging_data():
    """Verify staging data is available before transformation"""
    
    load_dotenv()
    
    # Add project root to Python path so we can import config
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from config import DB_CONNECTION_STRING

    # def create_db_connection():
    # # Create database connection using SQLAlchemy with connection string from config.py
    #     return create_engine(DB_CONNECTION_STRING)
    
    engine = engine = create_engine(DB_CONNECTION_STRING)

    with engine.connect() as conn:
        # Check artworks count
        artworks_count = conn.execute(
            text("SELECT COUNT(*) FROM staging.staging_moma_artworks")
        ).scalar()
        
        # Check artists count
        artists_count = conn.execute(
            text("SELECT COUNT(*) FROM staging.staging_moma_artists")
        ).scalar()
        
        print(f"Staging verification: {artworks_count:,} artworks, {artists_count:,} artists")
        
        if artworks_count == 0 or artists_count == 0:
            raise ValueError("Staging tables are empty. Load staging data first.")
        
        return True


def verify_transformations():
    """Verify all transformations completed successfully"""
    
    load_dotenv()
    
    # Add project root to Python path so we can import config
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from config import DB_CONNECTION_STRING

    # def create_db_connection():
    # # Create database connection using SQLAlchemy with connection string from config.py
    #     return create_engine(DB_CONNECTION_STRING)
    
    engine = engine = create_engine(DB_CONNECTION_STRING)
    
    with engine.connect() as conn:
        # Check transformed tables
        artworks_count = conn.execute(
            text("SELECT COUNT(*) FROM transformed.transformed_artworks")
        ).scalar()
        
        artists_count = conn.execute(
            text("SELECT COUNT(*) FROM transformed.transformed_artists")
        ).scalar()
        
        geo_count = conn.execute(
            text("SELECT COUNT(*) FROM transformed.transformed_geography")
        ).scalar()
        
        date_econ_count = conn.execute(
            text("SELECT COUNT(*) FROM transformed.transformed_date_economics")
        ).scalar()
        
        bridge_count = conn.execute(
            text("SELECT COUNT(*) FROM transformed.bridge_artwork_artist")
        ).scalar()
        
        print("="*60)
        print("TRANSFORMATION VERIFICATION RESULTS")
        print("="*60)
        print(f"Transformed artworks: {artworks_count:,}")
        print(f"Transformed artists: {artists_count:,}")
        print(f"Geographic records: {geo_count:,}")
        print(f"Date/Economic records: {date_econ_count:,}")
        print(f"Bridge relationships: {bridge_count:,}")
        print("="*60)
        
        # Validate minimum records
        if artworks_count < 100000 or artists_count < 10000:
            raise ValueError("Transformed tables have fewer records than expected")
        
        if bridge_count == 0:
            raise ValueError("Bridge table is empty")
        
        return True