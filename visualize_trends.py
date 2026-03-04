import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import os
from pathlib import Path

# 1. USAMOS RUTA RELATIVA (Igual que en extract_data.py)
db_path = Path("./data/crypto_warehouse.db")

if not db_path.exists():
    print(f"❌ Error: No encuentro la base de datos en {db_path.absolute()}")
    print("Asegúrate de haber ejecutado primero 'extract_data.py' y 'dbt run'.")
else:
    # Conectamos usando la ruta relativa convertida a texto
    con = duckdb.connect(str(db_path))
    
    print("🔍 Tablas encontradas en la base de datos:")
    try:
        tablas = con.execute("SHOW ALL TABLES").df()
        print(tablas)
    except Exception as e:
        print(f"No se pudieron listar las tablas: {e}")

    # Consulta a la capa 'Mart' (generada por dbt)
    query = """
        SELECT coin_name, price_usd, updated_at_timestamp 
        FROM mart_crypto_trends 
        ORDER BY updated_at_timestamp ASC
    """
    
    try:
        df = con.execute(query).df()
        
        if df.empty:
            print("⚠️ La tabla está vacía. ¿Has ejecutado 'dbt run'?")
        else:
            # Procesamiento de datos para la gráfica
            df['updated_at_timestamp'] = pd.to_datetime(df['updated_at_timestamp'])
            
            plt.figure(figsize=(10, 6))
            for coin in df['coin_name'].unique():
                subset = df[df['coin_name'] == coin]
                plt.plot(subset['updated_at_timestamp'], subset['price_usd'], marker='o', label=coin)
            
            plt.title('Evolución de Precios Crypto (Data from dbt Marts)')
            plt.xlabel('Fecha y Hora')
            plt.ylabel('Precio USD')
            plt.legend()
            plt.grid(True, linestyle='--', alpha=0.7)
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            print("📈 Generando gráfica...")
            plt.show()
        
    except Exception as e:
        print(f"\n❌ Error al ejecutar la consulta: {e}")
    finally:
        con.close()