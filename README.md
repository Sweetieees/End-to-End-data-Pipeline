# End-to-End-data-Pipeline
## Overview
This project implements an end-to-end data engineering pipeline using the MovieLens 32M dataset.
Raw CSV data is ingested, transformed, and stored in an analytics-ready format using PySpark.
SQL-based analytics are then performed on the processed data to generate business insights.

## Architecture
The pipeline follows a layered architecture:
- Raw data ingestion
- Distributed ETL processing with PySpark
- Partitioned Parquet storage
- SQL-based analytics using Spark SQL

## Tech Stack
- Python
- PySpark
- Spark SQL
- Parquet
- Git

## Dataset
- MovieLens 32M Dataset

## ETL Pipeline
- Ingests raw CSV files (ratings, movies)
- Cleans and normalizes schemas
- Enriches data with time-based features
- Joins fact and dimension tables
- Stores processed data as partitioned Parquet files

## Analytics
The analytics job computes:
- Top-rated movies
- Average rating per movie
- Daily Active Users (DAU)
- Most popular genres

## Performance Optimizations
- Cached frequently reused datasets to avoid recomputation
- Partitioned data by event_date to enable partition pruning
- Used broadcast joins for small dimension tables
- Analyzed execution plans using Spark explain()


## How to Run
```bash
# Run ETL
python spark/etl_job.py

# Run Analytics
python spark/analytics_job.py
