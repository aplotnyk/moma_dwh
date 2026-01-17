-- ============================================================
-- CREATE TRANSFORMED SCHEMA
-- ============================================================
CREATE SCHEMA IF NOT EXISTS transformed
    AUTHORIZATION postgres;

COMMENT ON SCHEMA transformed 
    IS 'Cleaned and standardized data ready for dimensional modeling';

-- ============================================================
-- TRANSFORMED ARTWORKS TABLE
-- ============================================================
CREATE TABLE transformed.transformed_artworks (
    -- Primary identifiers
    artwork_id INTEGER PRIMARY KEY,  -- Fixed: Added INTEGER
    
    -- Core artwork info
    title TEXT,
    creation_year INTEGER,
    creation_date_raw TEXT,
    date_certainty VARCHAR(20), -- 'exact', 'circa', 'range', 'unknown'
    
    -- Medium and classification
    medium TEXT,
    classification VARCHAR(100),
    department VARCHAR(100),
    
    -- Physical dimensions (cleaned and converted)
    height_cm DECIMAL(10,2),
    width_cm DECIMAL(10,2),
    depth_cm DECIMAL(10,2),  -- Fixed: Uncommented
    dimensions_text TEXT,
    
    -- Acquisition info
    date_acquired DATE,
    acquisition_year INTEGER,
    credit_line TEXT,
    acquisition_method VARCHAR(50), -- 'Gift', 'Purchase', 'Bequest', 'Unknown'
    accession_number VARCHAR(100),
    
    -- Status flags
    cataloged BOOLEAN,
    
    -- URLs
    moma_url TEXT,
    image_url TEXT,
    
    -- Data quality
    data_quality_score DECIMAL(3,2), -- 0.00 to 1.00
    
    -- Metadata
    transformed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_trans_artworks_artwork_id ON transformed.transformed_artworks(artwork_id);
CREATE INDEX idx_trans_artworks_creation_year ON transformed.transformed_artworks(creation_year);
CREATE INDEX idx_trans_artworks_acquisition_year ON transformed.transformed_artworks(acquisition_year);
CREATE INDEX idx_trans_artworks_department ON transformed.transformed_artworks(department);
CREATE INDEX idx_trans_artworks_classification ON transformed.transformed_artworks(classification);

COMMENT ON TABLE transformed.transformed_artworks 
    IS 'Cleaned and standardized artwork data with quality scores';

-- ============================================================
-- TRANSFORMED ARTISTS TABLE
-- ============================================================
CREATE TABLE transformed.transformed_artists (
    -- Primary identifiers
    artist_id INTEGER PRIMARY KEY,  -- Fixed: Added INTEGER
    
    -- Artist info
    display_name VARCHAR(200) NOT NULL,
    
    -- Dates (cleaned)
    birth_year INTEGER,
    death_year INTEGER,
    is_living BOOLEAN,
    lifespan_years INTEGER, -- calculated
    
    -- Nationality (standardized)
    nationality VARCHAR(100),
    
    -- Gender (standardized)
    gender_clean VARCHAR(20), -- 'Male', 'Female', 'Non-binary', 'Unknown'
    
    -- Wikidata enrichment
    wikidata_id VARCHAR(50),
    has_wikidata_enrichment BOOLEAN,
    art_movements TEXT[], -- Array of movements
    birth_place TEXT,
    birth_place_country TEXT,
    death_place TEXT,
    education TEXT,
    influenced_by TEXT[], -- Array of artist names
    
    -- Derived stats
    total_artworks_in_collection INTEGER DEFAULT 0,
    first_acquisition_year INTEGER,
    last_acquisition_year INTEGER,
    
    -- Data quality
    data_quality_score DECIMAL(3,2),
    
    -- Metadata
    transformed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_trans_artists_artist_id ON transformed.transformed_artists(artist_id);
CREATE INDEX idx_trans_artists_nationality ON transformed.transformed_artists(nationality);
CREATE INDEX idx_trans_artists_birth_year ON transformed.transformed_artists(birth_year);
CREATE INDEX idx_trans_artists_wikidata ON transformed.transformed_artists(wikidata_id);

COMMENT ON TABLE transformed.transformed_artists 
    IS 'Cleaned and enriched artist data with geographic and movement context';

-- ============================================================
-- BRIDGE TABLE: ARTWORK-ARTIST RELATIONSHIPS
-- ============================================================
CREATE TABLE transformed.bridge_artwork_artist (
    artwork_id INTEGER NOT NULL,
    artist_id INTEGER NOT NULL,
    artists_total INTEGER,
    
    CONSTRAINT bridge_artwork_artist_pkey PRIMARY KEY (artwork_id, artist_id),  -- Fixed: Added comma
    CONSTRAINT fk_artwork FOREIGN KEY (artwork_id)
        REFERENCES transformed.transformed_artworks(artwork_id) ON DELETE CASCADE,
    CONSTRAINT fk_artist FOREIGN KEY (artist_id)
        REFERENCES transformed.transformed_artists(artist_id) ON DELETE CASCADE
);

CREATE INDEX idx_bridge_artwork ON transformed.bridge_artwork_artist(artwork_id);
CREATE INDEX idx_bridge_artist ON transformed.bridge_artwork_artist(artist_id);

COMMENT ON TABLE transformed.bridge_artwork_artist 
    IS 'Many-to-many relationship between artworks and artists';

-- ============================================================
-- TRANSFORMED GEOGRAPHY TABLE
-- ============================================================
CREATE TABLE transformed.transformed_geography (
    -- Primary key: nationality for linking to artists
    nationality VARCHAR(100) PRIMARY KEY,
    
    -- Country identification
    country_name VARCHAR(100) NOT NULL,
    country_code_iso2 VARCHAR(2),
    country_code_iso3 VARCHAR(3),
    
    -- Geographic hierarchy
    continent VARCHAR(50),
    region VARCHAR(100),
    subregion VARCHAR(100),
    
    -- Coordinates
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    
    -- Aggregated statistics
    total_moma_artists INTEGER DEFAULT 0,
    total_moma_artworks INTEGER DEFAULT 0,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_trans_geo_country_name ON transformed.transformed_geography(country_name);
CREATE INDEX idx_trans_geo_continent ON transformed.transformed_geography(continent);
CREATE INDEX idx_trans_geo_region ON transformed.transformed_geography(region);

COMMENT ON TABLE transformed.transformed_geography 
    IS 'Geographic reference data keyed by nationality';

COMMENT ON COLUMN transformed.transformed_geography.nationality 
    IS 'Nationality adjective - primary key for linking to transformed_artists';

-- ============================================================
-- TRANSFORMED DATE ECONOMICS TABLE
-- ============================================================
CREATE TABLE transformed.transformed_date_economics (
    -- Date key: Format YYYYMMDD (e.g., 19291115 for Nov 15, 1929)
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    
    -- Time periods (derived from year)
    decade INTEGER NOT NULL,       -- 1920, 1930, 1940, etc.
    century INTEGER NOT NULL,      -- 19, 20, 21
    era VARCHAR(50),               -- 'Pre-WWII', 'WWII', 'Post-WWII', etc.
    
    -- Economic indicators (from FRED staging data)
    gdp_usd_billions DECIMAL(12,2),
    inflation_rate_percent DECIMAL(5,2),
    unemployment_rate_percent DECIMAL(5,2),
    sp500_close DECIMAL(10,2),
    
    -- Economic classification
    economic_period VARCHAR(50),   -- 'Recession', 'Expansion'
    
    -- Historical events
    major_events TEXT,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_trans_date_year ON transformed.transformed_date_economics(year);
CREATE INDEX idx_trans_date_decade ON transformed.transformed_date_economics(decade);
CREATE INDEX idx_trans_date_economic_period ON transformed.transformed_date_economics(economic_period);
CREATE INDEX idx_trans_date_era ON transformed.transformed_date_economics(era);
CREATE INDEX idx_trans_date_full_date ON transformed.transformed_date_economics(full_date);

COMMENT ON TABLE transformed.transformed_date_economics 
    IS 'Economic and historical context indexed by year';

COMMENT ON COLUMN transformed.transformed_date_economics.full_date 
    IS 'Full date stored as January 1 of each year (YYYY-01-01)';

COMMENT ON COLUMN transformed.transformed_date_economics.gdp_usd_billions 
    IS 'Real GDP in billions from FRED staging data';

-- ============================================================
-- VERIFY SCHEMA CREATION
-- ============================================================
SELECT 
    schemaname,
    tablename,
    (SELECT COUNT(*) 
     FROM information_schema.columns 
     WHERE table_schema = t.schemaname 
       AND table_name = t.tablename) as column_count
FROM pg_tables t
WHERE schemaname = 'transformed'
ORDER BY tablename;