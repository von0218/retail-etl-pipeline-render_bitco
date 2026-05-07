import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# 1. Setup Connection
load_dotenv()
DB_URL = os.getenv('DATABASE_URL')
engine = create_engine(DB_URL)

def run_etl():
    try:
        # --- STEP 1: EXTRACT (Staging Layer) ---
        print("--- 🚀 Extraction Phase ---")
        # Matching the exact filenames from your screenshot
        df_japan = pd.read_csv('retail_sales_dataset.csv')
        df_myanmar = pd.read_csv('SuperMarket Analysis.csv')
        
        # Load raw data to staging tables in Render
        df_japan.to_sql('stg_japan_sales', engine, if_exists='replace', index=False)
        df_myanmar.to_sql('stg_myanmar_sales', engine, if_exists='replace', index=False)
        print("Staging complete: Raw data uploaded to PostgreSQL.")

        # --- STEP 2: TRANSFORM (Transformation Layer) ---
        print("--- ⚙️ Transformation Phase ---")
        
        # A. Clean Japan Data
        # Assuming Kaggle 'Price' column; adjust if names differ
        # Standardizing JPY to USD (1 JPY ≈ 0.0067 USD)
        if 'Price' in df_japan.columns:
            df_japan['price_usd'] = df_japan['Price'] * 0.0067
        elif 'price' in df_japan.columns:
            df_japan['price_usd'] = df_japan['price'] * 0.0067
            
        df_japan['store_location'] = 'Japan'
        
        # B. Clean Myanmar Data
        # Myanmar Kaggle data usually has 'Total' or 'Unit price'
        if 'Total' in df_myanmar.columns:
            df_myanmar['price_usd'] = df_myanmar['Total']
        elif 'price' in df_myanmar.columns:
            df_myanmar['price_usd'] = df_myanmar['price']
            
        df_myanmar['store_location'] = 'Myanmar'
        
        # Drop rows with missing crucial data
        df_japan = df_japan.dropna()
        df_myanmar = df_myanmar.dropna()
        
        print("Transformation complete: Currency standardized and nulls removed.")

        # --- STEP 3: LOAD (Presentation Layer) ---
        print("--- 📊 Loading Phase ---")
        # Combine both datasets for a global view
        # We use 'inner' join on columns or just concat if we want the full history
        final_df = pd.concat([df_japan, df_myanmar], ignore_index=True)
        
        # Save to the final analytics table
        final_df.to_sql('fact_global_sales', engine, if_exists='replace', index=False)
        
        print("✅ ETL SUCCESS: Final table 'fact_global_sales' is ready in Render.")

    except Exception as e:
        print(f"❌ Error occurred: {e}")

if __name__ == "__main__":
    run_etl()
