import pandas as pd
import os
import sys
from sqlalchemy import create_engine, text
import http.server
import socketserver

# Only load dotenv if we are NOT on Render
if not os.getenv('RENDER'):
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportWarning:
        pass

def run_etl():
    # 1. Get the URL from Render's Environment Variables
    DB_URL = os.getenv('DATABASE_URL')
    
    if not DB_URL:
        print("❌ ERROR: DATABASE_URL environment variable is missing!")
        return

    try:
        print("--- 🚀 Extraction Phase ---")
        # Ensure filenames match your GitHub files exactly
        df_j = pd.read_csv('retail_sales_dataset.csv')
        df_m = pd.read_csv('SuperMarket Analysis.csv')
        
        print("--- ⚙️ Transformation Phase ---")
        # (Your transformation logic remains the same...)
        df_j['store_location'] = 'Japan'
        df_m['store_location'] = 'Myanmar'
        
        # Simple price cleaning for Japan
        j_col = [c for c in df_j.columns if 'Price' in c][0]
        df_j['price_usd'] = pd.to_numeric(df_j[j_col], errors='coerce') * 0.0067
        
        # Simple price cleaning for Myanmar
        m_col = [c for c in df_m.columns if 'Total' in c][0]
        df_m['price_usd'] = pd.to_numeric(df_m[m_col], errors='coerce')

        print("--- 📊 Loading Phase ---")
        final_df = pd.concat([df_j, df_m], ignore_index=True).dropna(subset=['price_usd'])
        
        engine = create_engine(DB_URL)
        final_df.to_sql('fact_global_sales', engine, if_exists='replace', index=False)
        print(f"✅ SUCCESS: Uploaded {len(final_df)} rows!")

    except Exception as e:
        print(f"❌ ETL ERROR: {str(e)}")
        # We don't 'raise' the error so the script can keep moving to the server
        
def start_dummy_server():
    PORT = int(os.environ.get("PORT", 8080))
    # Allow port reuse
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("", PORT), http.server.SimpleHTTPRequestHandler) as httpd:
        print(f"✅ Web Server Live on Port {PORT}")
        httpd.serve_forever()

if __name__ == "__main__":
    print("--- 🏁 Script Initializing ---")
    run_etl()
    start_dummy_server()