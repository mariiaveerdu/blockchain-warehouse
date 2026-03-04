import requests
import pandas as pd
import duckdb
import os
from datetime import datetime
from pathlib import Path
# NUEVO: Para poder leer archivos .env si decides usarlos
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

def run_ingestion():
    # 1. CONFIGURACIÓN DE RUTAS Y CONEXIÓN (Modificado para MotherDuck)
    token = os.getenv("motherduck_token")
    
    if token:
        # Fíjate que esto tenga 8 espacios (o 2 tabulaciones) desde el inicio de la línea
        db_url = f"md:crypto_db?motherduck_token={token}"
        print("☁️ Conectando a la nube (MotherDuck)...")
    else:
        db_path = Path("./data/crypto_warehouse.db")
        db_path.parent.mkdir(parents=True, exist_ok=True)
        db_url = str(db_path)
        print(f"🏠 Conectando a base de datos local: {db_path.absolute()}")

    try:
        # 2. EXTRACCIÓN (API de CoinGecko) - Igual
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            "ids": "bitcoin,ethereum,solana",
            "vs_currencies": "usd",
            "include_market_cap": "true",
            "include_24hr_vol": "true",
            "include_last_updated_at": "true"
        }
        
        response = requests.get(url, params=params)
        response.raise_for_status() 
        data = response.json()

        # 3. TRANSFORMACIÓN BÁSICA (Pandas) - Igual
        df = pd.DataFrame.from_dict(data, orient='index').reset_index()
        df.columns = ['coin_id', 'price_usd', 'market_cap', 'vol_24h', 'last_updated_at']
        df['extracted_at'] = datetime.now()

        # 4. CARGA A DUCKDB/MOTHERDUCK
        # Usamos la db_url que decidimos en el paso 1
        con = duckdb.connect(db_url)

        con.execute("""
            CREATE TABLE IF NOT EXISTS raw_crypto (
                coin_id VARCHAR,
                price_usd DOUBLE,
                market_cap DOUBLE,
                vol_24h DOUBLE,
                last_updated_at BIGINT,
                extracted_at TIMESTAMP
            )
        """)

        con.execute("INSERT INTO raw_crypto SELECT * FROM df")
        
        count = con.execute("SELECT COUNT(*) FROM raw_crypto").fetchone()[0]
        
        print(f"✅ Éxito: Se han insertado {len(df)} registros.")
        print(f"📊 Total de registros en raw_crypto: {count}")
        
        con.close()

    except Exception as e:
        print(f"❌ Error durante la ingesta: {e}")

if __name__ == "__main__":
    run_ingestion()