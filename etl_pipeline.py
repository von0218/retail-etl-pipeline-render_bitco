import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv('DATABASE_URL')
engine = create_engine(DB_URL)

def run_etl():
    try:
        print("--- 🚀 Extraction Phase ---")
        df_j = pd.read_csv('retail_sales_dataset.csv')
        df_m = pd.read_csv('SuperMarket Analysis.csv')
        
        print("--- ⚙️ Transformation Phase ---")
        
        # Clean Japan Price 
        # Find the column that contains 'Price' or use the 4th column as fallback
        j_price_col = [c for c in df_j.columns if 'Price' in c or 'price' in c]
        j_col = j_price_col[0] if j_price_col else df_j.columns[3]
        
        # Convert to string, remove commas/currency symbols, and convert to numeric
        df_j[j_col] = df_j[j_col].astype(str).str.replace(r'[^\d.]', '', regex=True)
        df_j['price_clean'] = pd.to_numeric(df_j[j_col], errors='coerce')
        
        # Calculate Japan USD (1 JPY = 0.0067 USD)
        df_j['price_usd'] = df_j['price_clean'] * 0.0067
        df_j['store_location'] = 'Japan'
        
        # --- CLEAN MYANMAR PRICE ---
        # Find the column that contains 'Total' or use the last column as fallback
        m_price_col = [c for c in df_m.columns if 'Total' in c or 'total' in c]
        m_col = m_price_col[0] if m_price_col else df_m.columns[-1]
        
        # Convert to string, clean, and convert to numeric
        df_m[m_col] = df_m[m_col].astype(str).str.replace(r'[^\d.]', '', regex=True)
        df_m['price_clean'] = pd.to_numeric(df_m[m_col], errors='coerce')
        
        # Myanmar data is already in USD/Normalized
        df_m['price_usd'] = df_m['price_clean']
        df_m['store_location'] = 'Myanmar'

        # Drop rows where price conversion failed (NaNs)
        df_j = df_j.dropna(subset=['price_usd'])
        df_m = df_m.dropna(subset=['price_usd'])

        print("--- 📊 Loading Phase ---")
        final_df = pd.concat([df_j, df_m], ignore_index=True)
        
        # Clean up temporary helper column before loading
        if 'price_clean' in final_df.columns:
            final_df = final_df.drop(columns=['price_clean'])

        # Drop the table in Render so it recreates fresh with the new schema
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS fact_global_sales CASCADE;"))
            conn.commit()
            print("Old table dropped from Render.")

        # Upload the clean dataset
        final_df.to_sql('fact_global_sales', engine, if_exists='replace', index=False)
        print(f"✅ SUCCESS: Uploaded {len(final_df)} rows!")
        print(f"Columns now in database: {final_df.columns.tolist()}")

    except Exception as e:
        print(f"❌ Error during ETL: {e}")

if __name__ == "__main__":
    run_etl()
