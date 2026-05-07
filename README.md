# Retail ETL Pipeline 

## Project Overview
This project demonstrates an ETL (Extract, Transform, Load) pipeline that migrates retail data from local CSV sources to a **PostgreSQL** instance hosted on **Render**. 

## The Process
1. **Extract**: Loads raw data into the `stg_` (staging) tables.
2. **Transform**: Cleans data using Pandas and standardizes currency to USD.
3. **Load**: Consolidates data into a final `fact_global_sales` table for analytics.

## Tech Stack
* **Language**: Python (Pandas)
* **Database**: PostgreSQL (Render)
* **Tooling**: SQLAlchemy, pgAdmin 4
