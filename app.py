import streamlit as st
import duckdb
import os
import pandas as pd

# Configuración de la página
st.set_page_config(page_title="Crypto Tracker", page_icon="📈")

st.title("🚀 Crypto Data Dashboard")
st.markdown("Este dashboard se actualiza automáticamente cada día usando **GitHub Actions**, **dbt** y **MotherDuck**.")

# Conexión a MotherDuck
# Usamos el secreto que configuraremos en Streamlit Cloud
token = os.getenv('motherduck_token')
if not token:
    st.error("No se encontró el Token de MotherDuck. Configúralo en los Secrets.")
else:
    con = duckdb.connect(f"md:crypto_db?motherduck_token={token}")
    
    # Cargar datos
    df = con.execute("SELECT * FROM mart_crypto_trends").df()

    # Métricas principales
    col1, col2 = st.columns(2)
    top_coin = df.iloc[0]
    col1.metric("Top Gainer", top_coin['coin_name'], f"{top_coin['pct_change']}%")
    col2.metric("Total Monedas", len(df))

    # Gráfico interactivo
    st.subheader("Variación de Precio (%) - Últimas 24h")
    st.bar_chart(data=df, x="coin_name", y="pct_change", color="#ff4b4b")

    # Tabla de datos
    st.subheader("Datos Detallados")
    st.dataframe(df[['coin_name', 'price_usd', 'pct_change', 'extracted_at']], use_container_width=True)