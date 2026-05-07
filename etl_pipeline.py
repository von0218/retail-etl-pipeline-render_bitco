import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load credentials
load_dotenv()
DB_URL = os.getenv('DATABASE_URL')
engine = create_engine(DB_URL)

def run_etl():
    # STEP 1: EXTRACT (Staging) 
    print("Extracting data from sources...")
    # Update these filenames if they are inside a 'data/' folder
    df_japan = pd.read_csv('Japan_store.csv')
    df_myanmar = pd.read_csv('Myanmar_store.csv')
    
    # Load raw data to staging tables in Postgres
    df_japan.to_sql('stg_japan_sales', engine, if_exists='replace', index=False)
    df_myanmar.to_sql('stg_myanmar_sales', engine, if_exists='replace', index=False)

    # STEP 2: TRANSFORM (Transformation) 
    print("Transforming and cleaning data...")
    # 1. Standardize Japan (Assume JPY to USD conversion 0.0067)
    df_japan['price_usd'] = df_japan['price'] * 0.0067
    df_japan['store_location'] = 'Japan'
    
    # 2. Standardize Myanmar (Assume already USD or convert as needed)
    df_myanmar['price_usd'] = df_myanmar['price']
    df_myanmar['store_location'] = 'Myanmar'
    
    # 3. Data Cleaning (Handling nulls/types)
    df_japan = df_japan.dropna()
    df_myanmar = df_myanmar.dropna()

    # STEP 3: LOAD (Presentation) 
    print("Consolidating into Presentation Layer...")
    # Combine both datasets
    final_df = pd.concat([df_japan, df_myanmar], ignore_index=True)
    
    # Save to the final analytics table
    final_df.to_sql('fact_global_sales', engine, if_exists='replace', index=False)
    print("ETL Complete! Data is ready in Render PostgreSQL.")

if __name__ == "__main__":
    run_etl()
