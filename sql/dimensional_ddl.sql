-- ============================================================
-- CREATE DIMENSIONAL SCHEMA
-- ============================================================
CREATE SCHEMA IF NOT EXISTS dimensional
    AUTHORIZATION postgres;

COMMENT ON SCHEMA dimensional 
    IS 'Star schema data warehouse - optimized for analytics';

-- ============================================================
-- DIMENSION TABLE: dim_date
-- ============================================================
CREATE TABLE dimensional.dim_date (
    date_key INTEGER PRIMARY KEY,
    
    -- Full date
    full_date DATE NOT NULL UNIQUE,
    
    -- Date components
    year INTEGER NOT NULL,
    decade INTEGER NOT NULL,
    century INTEGER NOT NULL,
    era VARCHAR(50),
    
    -- Economic context
    gdp_usd_billions DECIMAL(12,2),
    inflation_rate_percent DECIMAL(5,2),
    unemployment_rate_percent DECIMAL(5,2),
    sp500_close DECIMAL(10,2),
    economic_period VARCHAR(50),
    major_events TEXT,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_date_year ON dimensional.dim_date(year);
CREATE INDEX idx_dim_date_decade ON dimensional.dim_date(decade);
CREATE INDEX idx_dim_date_economic_period ON dimensional.dim_date(economic_period);
CREATE INDEX idx_dim_date_era ON dimensional.dim_date(era);

COMMENT ON TABLE dimensional.dim_date 
    IS 'Date dimension with economic and historical context';

-- ============================================================
-- DIMENSION TABLE: dim_geography
-- ============================================================
CREATE TABLE dimensional.dim_geography (
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
    
    -- Aggregated stats (denormalized for performance)
    total_artists INTEGER DEFAULT 0,
    total_artworks INTEGER DEFAULT 0,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_geo_country ON dimensional.dim_geography(country_name);
CREATE INDEX idx_dim_geo_continent ON dimensional.dim_geography(continent);
CREATE INDEX idx_dim_geo_region ON dimensional.dim_geography(region);

COMMENT ON TABLE dimensional.dim_geography 
    IS 'Geographic dimension keyed by nationality for direct artist/fact table joins';

-- ============================================================
-- DIMENSION TABLE: dim_artist (SCD Type 0 - No History)
-- ============================================================
CREATE TABLE dimensional.dim_artist (
    artist_id INTEGER PRIMARY KEY,
    
    -- Artist identification
    display_name VARCHAR(200) NOT NULL,
    
    -- Demographics
    birth_year INTEGER,
    death_year INTEGER,
    is_living BOOLEAN,
    lifespan_years INTEGER,
    
    -- Geographic - nationality is foreign key to dim_geography
    nationality VARCHAR(100),
    birth_place VARCHAR(200),
    death_place VARCHAR(200),
    
    -- Gender
    gender VARCHAR(20),
    
    -- Art movements and enrichment
    art_movements TEXT[],  -- Array of art movements
    wikidata_id VARCHAR(50),
    education TEXT,
    influenced_by TEXT[],
    
    -- Collection statistics (denormalized)
    total_artworks INTEGER DEFAULT 0,
    first_acquisition_year INTEGER,
    last_acquisition_year INTEGER,
    
    -- Data quality
    data_quality_score DECIMAL(3,2),
    has_wikidata_enrichment BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_artist_nationality ON dimensional.dim_artist(nationality);
CREATE INDEX idx_dim_artist_birth_year ON dimensional.dim_artist(birth_year);
CREATE INDEX idx_dim_artist_gender ON dimensional.dim_artist(gender);
CREATE INDEX idx_dim_artist_wikidata ON dimensional.dim_artist(wikidata_id);

COMMENT ON TABLE dimensional.dim_artist 
    IS 'Artist dimension (SCD Type 0) - no history tracking';

COMMENT ON COLUMN dimensional.dim_artist.artist_id 
    IS 'Primary key - natural key from transformed_artists';

COMMENT ON COLUMN dimensional.dim_artist.nationality 
    IS 'Foreign key to dim_geography - via nationality column';

COMMENT ON COLUMN dimensional.dim_artist.art_movements 
    IS 'Array of art movements - use UNNEST() for movement-based analytics';

-- ============================================================
-- DIMENSION TABLE: dim_artwork
-- ============================================================
CREATE TABLE dimensional.dim_artwork (
    -- Primary key
    artwork_id INTEGER PRIMARY KEY,
    
    -- Artwork identification
    title TEXT,
    accession_number VARCHAR(100),
    
    -- Physical attributes
    medium TEXT,
    classification VARCHAR(100),
    department VARCHAR(100),
    
    -- Dimensions
    height_cm DECIMAL(10,2),
    width_cm DECIMAL(10,2),
    depth_cm DECIMAL(10,2),
    dimensions_text TEXT,
    
    -- Creation info
    creation_year INTEGER,
    creation_date_raw TEXT,
    date_certainty VARCHAR(20),
    creation_decade INTEGER,
    creation_century INTEGER,
    
    -- Acquisition info
    acquisition_method VARCHAR(50),
    credit_line TEXT,
    
    -- Status
    cataloged BOOLEAN DEFAULT FALSE,
    
    -- URLs
    moma_url TEXT,
    image_url TEXT,
    
    -- Data quality
    data_quality_score DECIMAL(3,2),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_artwork_artwork_id ON dimensional.dim_artwork(artwork_id);
CREATE INDEX idx_dim_artwork_medium ON dimensional.dim_artwork(medium);
CREATE INDEX idx_dim_artwork_department ON dimensional.dim_artwork(department);
CREATE INDEX idx_dim_artwork_creation_year ON dimensional.dim_artwork(creation_year);
CREATE INDEX idx_dim_artwork_creation_decade ON dimensional.dim_artwork(creation_decade);
CREATE INDEX idx_dim_artwork_classification ON dimensional.dim_artwork(classification);

COMMENT ON TABLE dimensional.dim_artwork 
    IS 'Artwork dimension with physical and descriptive attributes';

-- ============================================================
-- FACT TABLE: fact_artwork_acquisitions
-- ============================================================
CREATE TABLE dimensional.fact_artwork_acquisitions (
    acquisition_key SERIAL PRIMARY KEY,
    
    -- Foreign keys to dimensions
    artwork_id INTEGER NOT NULL REFERENCES dimensional.dim_artwork(artwork_id),
    artist_id INTEGER NOT NULL REFERENCES dimensional.dim_artist(artist_id),
    acquisition_date_key INTEGER REFERENCES dimensional.dim_date(date_key),
    creation_date_key INTEGER REFERENCES dimensional.dim_date(date_key),
    nationality VARCHAR(100),
    
    -- Degenerate dimensions
    accession_number VARCHAR(100),
    
    -- Numeric facts
    years_from_creation_to_acquisition INTEGER,
    
    -- Flags
    is_gift BOOLEAN DEFAULT FALSE,
    is_purchase BOOLEAN DEFAULT FALSE,
    is_bequest BOOLEAN DEFAULT FALSE,
    is_living_artist_at_acquisition BOOLEAN DEFAULT FALSE,
    is_contemporary BOOLEAN DEFAULT FALSE,
    is_cataloged BOOLEAN DEFAULT FALSE,
    is_female_artist BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_acq_artwork ON dimensional.fact_artwork_acquisitions(artwork_id);
CREATE INDEX idx_fact_acq_artist ON dimensional.fact_artwork_acquisitions(artist_id);
CREATE INDEX idx_fact_acq_acq_date ON dimensional.fact_artwork_acquisitions(acquisition_date_key);
CREATE INDEX idx_fact_acq_creation_date ON dimensional.fact_artwork_acquisitions(creation_date_key);
CREATE INDEX idx_fact_acq_nationality ON dimensional.fact_artwork_acquisitions(nationality);

COMMENT ON TABLE dimensional.fact_artwork_acquisitions 
    IS 'Fact table: one row per artwork acquisition with all context';
-- ============================================================
-- FACT TABLE: fact_artist_summary (Periodic Snapshot)
-- ============================================================
CREATE TABLE dimensional.fact_artist_summary (
    artist_summary_key SERIAL PRIMARY KEY,
    
    -- Foreign keys
    artist_id INTEGER NOT NULL REFERENCES dimensional.dim_artist(artist_id),
    nationality VARCHAR(100),
    snapshot_date DATE NOT NULL DEFAULT CURRENT_DATE,
    
    -- Aggregate measures
    total_artworks INTEGER DEFAULT 0,
    
    -- Temporal measures
    first_acquisition_year INTEGER,
    last_acquisition_year INTEGER,
    span_of_acquisitions_years INTEGER,
    earliest_artwork_year INTEGER,
    latest_artwork_year INTEGER,
    
    -- Statistical measures
    avg_years_to_acquisition DECIMAL(5,2),
    
    -- Flags
    has_contemporary_works BOOLEAN DEFAULT FALSE,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_artist_snapshot UNIQUE (artist_id, snapshot_date)
);

CREATE INDEX idx_fact_summary_artist ON dimensional.fact_artist_summary(artist_id);
CREATE INDEX idx_fact_summary_nationality ON dimensional.fact_artist_summary(nationality);
CREATE INDEX idx_fact_summary_snapshot ON dimensional.fact_artist_summary(snapshot_date);

COMMENT ON TABLE dimensional.fact_artist_summary 
    IS 'Aggregate fact table: one row per artist per snapshot date';

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
WHERE schemaname = 'dimensional'
ORDER BY CASE
    WHEN tablename LIKE 'dim_%' THEN 1
    WHEN tablename LIKE 'fact_%' THEN 2
    ELSE 3
END,
tablename;