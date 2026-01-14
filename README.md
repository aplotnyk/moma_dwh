# 📊 MoMA Data Warehouse Project

## **📖 Project Overview**

This project is a **Data Warehouse and analytics pipeline** orchestrated using **Apache Airflow**.

It ingests public datasets from the **Museum of Modern Art (MoMA)** and enriches them with external contextual data from multiple APIs to enable analytical reporting on artists, artworks, and their historical and economic context.

The pipeline follows a **multi-layered Data Warehouse architecture**, progressing from raw ingestion to analytics-ready dimensional models.

---

## Data Sources

### Primary Source

- **MoMA GitHub Repository**
    - `Artists.csv`
    - `Artworks.csv`

### External APIs

- **Wikidata API**
    - Artist metadata (e.g., birth/death dates & locations, movement, education, influenced by)
- **FRED API**
    - Economic indicators for selected years (e.g., GDP, inflation, unemployment)
- **REST Countries API**
    - Country-level information (e.g., continent, region, ISO codes)

---

## Orchestration & Workflow

- **Apache Airflow** is used to:
    - Orchestrate ingestion and enrichment pipelines
    - Manage dependencies between datasets
    - Handle retries, scheduling, and monitoring
    - Separate raw ingestion from transformation and modeling steps

Each data source is processed through dedicated DAGs with clearly defined task boundaries.

---

## Data Warehouse Architecture

The Data Warehouse is organized into **three schemas**:

### 1. Staging (Raw Layer)

- Stores raw ingested data
- Minimal or no transformation
- Mirrors source structures
- Used for traceability and debugging

### 2. Transformed (Clean Layer)

- Cleaned, standardized, and filtered datasets
- Deduplicated records
- Normalized fields (dates, country codes, identifiers)
- Enriched with API data

### 3. Dimensional (Analytics Layer)

- Star-schema–style tables
- Fact and dimension tables
- Optimized for BI tools and analytical queries
- Ready for reporting and insights

---

## Key Features

- End-to-end automated data pipeline
- Multi-source data enrichment
- Layered Data Warehouse design
- Scalable Airflow DAG structure
- Separation of ingestion, transformation, and analytics

---

## Technologies Used

- **Apache Airflow**
- **Python**
- **SQL**
- **REST APIs**
- **PostgreSQL / Data Warehouse**

---
