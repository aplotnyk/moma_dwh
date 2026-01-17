-- ============================================================
-- CREATE STAGING SCHEMA
-- ============================================================
CREATE SCHEMA IF NOT EXISTS staging
    AUTHORIZATION postgres;

COMMENT ON SCHEMA staging 
    IS 'Landing zone for raw data from external sources';

-- ============================================================
-- STAGING TABLE 1: MoMA Artworks Dataset
-- ============================================================
CREATE TABLE staging.staging_moma_artworks (
    title TEXT,
    artist TEXT,
    constituent_id TEXT,
    artist_bio TEXT,
    nationality TEXT,
    begin_date TEXT,
    end_date TEXT,
    gender TEXT,
    date TEXT,
    medium TEXT,
    dimensions TEXT,
    credit_line TEXT,
    accession_number TEXT,
    classification TEXT,
    department TEXT,
    date_acquired TEXT,
    cataloged TEXT,
    object_id TEXT PRIMARY KEY,
    url TEXT,
    image_url TEXT,
    on_view TEXT,
    
    -- Physical dimensions (all as TEXT initially)
    circumference_cm TEXT,
    depth_cm TEXT,
    diameter_cm TEXT,
    height_cm TEXT,
    length_cm TEXT,
    weight_kg TEXT,
    width_cm TEXT,
    seat_height_cm TEXT,
    duration_sec TEXT,
    
    -- Metadata fields (added, not in original CSV)
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255)
);

-- Add comments for documentation
COMMENT ON TABLE staging.staging_moma_artworks 
    IS 'Raw MoMA artworks data from GitHub CSV - no transformations applied';

-- Create index on object_id for faster lookups
CREATE INDEX idx_staging_artworks_object_id 
    ON staging.staging_moma_artworks(object_id);

-- ============================================================
-- STAGING TABLE 2: MoMA Artists Dataset
-- ============================================================
CREATE TABLE staging.staging_moma_artists (
    -- Core artist information
    constituent_id TEXT PRIMARY KEY,
    display_name TEXT,
    artist_bio TEXT,
    nationality TEXT,
    gender TEXT,
    begin_date TEXT,
    end_date TEXT,
    wiki_qid TEXT,
    ulan TEXT,
    
    -- Metadata fields
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255)
);

-- Add comments
COMMENT ON TABLE staging.staging_moma_artists 
    IS 'Raw MoMA artists data from GitHub CSV - no transformations applied';

-- Create index on constituent_id for joins
CREATE INDEX idx_staging_artists_constituent_id 
    ON staging.staging_moma_artists(constituent_id);

-- Create index on display_name for searches
CREATE INDEX idx_staging_artists_display_name 
    ON staging.staging_moma_artists(display_name);

-- ============================================================
-- STAGING TABLE 3: Wikidata Artists Enrichment
-- ============================================================
CREATE TABLE staging.staging_wikidata_artists (
    -- Identification
    artist_name TEXT,
    wikidata_id TEXT PRIMARY KEY,  -- Fixed: Added TEXT data type
    
    -- Biographical data
    movements TEXT[],  -- Array of art movements
    bio_text TEXT,
    birth_place TEXT,
    birth_place_country TEXT,
    death_place TEXT,
    education TEXT,
    influenced_by TEXT[],  -- Array of artist names
    
    -- Raw API response (for debugging/reprocessing)
    api_response_json JSONB,
    
    -- Metadata
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fetch_status VARCHAR(50)  -- success, failed, not_found
);

CREATE INDEX idx_wikidata_artist_name ON staging.staging_wikidata_artists(artist_name);
CREATE INDEX idx_wikidata_id ON staging.staging_wikidata_artists(wikidata_id);

COMMENT ON TABLE staging.staging_wikidata_artists 
    IS 'Enrichment data from Wikidata API for MoMA artists';

-- ============================================================
-- STAGING TABLE 4: Geographic Reference Data
-- ============================================================
CREATE TABLE staging.staging_geographic_reference (
    -- Geographic identifiers
    country_name TEXT PRIMARY KEY,
    country_code_iso2 CHAR(2),
    country_code_iso3 CHAR(3),
    
    -- Geographic hierarchy
    continent TEXT,
    region TEXT,
    subregion TEXT,
    
    -- Coordinates
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    
    -- Nationality adjective
    nationality TEXT,
    
    -- Metadata
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(100)
);

CREATE INDEX idx_geo_country_code ON staging.staging_geographic_reference(country_code_iso2);
CREATE INDEX idx_geo_continent ON staging.staging_geographic_reference(continent);
CREATE INDEX idx_geo_nationality ON staging.staging_geographic_reference(nationality);

COMMENT ON TABLE staging.staging_geographic_reference 
    IS 'Country to continent/region mapping for geographic analysis';

COMMENT ON COLUMN staging.staging_geographic_reference.nationality
    IS 'Nationality adjective (e.g., American, French) - links to artist nationality';

-- ============================================================
-- STAGING TABLE 5: Economic Context Data
-- ============================================================
CREATE TABLE staging.staging_economic_data (
    -- Time identifier
    year INTEGER PRIMARY KEY,
    
    -- Economic indicators
    gdp_usd_billions DECIMAL(12,2),
    inflation_rate_percent DECIMAL(5,2),
    unemployment_rate_percent DECIMAL(5,2),
    sp500_close DECIMAL(10,2),
    
    -- Context
    major_events TEXT,
    economic_period VARCHAR(50),  -- recession, expansion, recovery
    
    -- Metadata
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(100)
);

CREATE INDEX idx_economic_year ON staging.staging_economic_data(year);

COMMENT ON TABLE staging.staging_economic_data 
    IS 'Historical economic data from FRED and other sources';

-- ============================================================
-- Verify All Tables Created
-- ============================================================
SELECT 
    table_name,
    (SELECT COUNT(*) 
     FROM information_schema.columns 
     WHERE table_schema = 'staging' 
       AND table_name = t.table_name) as column_count
FROM information_schema.tables t
WHERE table_schema = 'staging'
ORDER BY table_name;