import streamlit as st
import duckdb
import os
import pandas as pd

# 1. Page Configuration
st.set_page_config(page_title="Crypto Analytics Dashboard", page_icon="📊", layout="wide")

# Custom CSS to make it look cleaner
st.markdown("""
    <style>
    .main {
        background-color: #f5f7f9;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    </style>
    """, unsafe_allow_html=True)

st.title("📈 Market Intelligence Dashboard")
st.subheader("Automated Daily Crypto Insights")
st.divider()

# 2. Connection to MotherDuck
token = os.getenv('motherduck_token')
if not token:
    st.error("MotherDuck Token not found. Please check your Secrets.")
else:
    con = duckdb.connect(f"md:crypto_db?motherduck_token={token}")
    
    # Fetch Data
    df = con.execute("SELECT * FROM mart_crypto_trends").df()

    # 3. Top Metrics (KPIs)
    col1, col2, col3, col4 = st.columns(4)
    
    # Look for Bitcoin specifically
    btc = df[df['coin_name'] == 'Bitcoin'].iloc[0] if 'Bitcoin' in df['coin_name'].values else None
    
    with col1:
        if btc is not None:
            st.metric("Bitcoin (BTC)", f"${btc['price_usd']:,.0f}", f"{btc['pct_change']}%")
    
    with col2:
        top_gainer = df.loc[df['pct_change'].idxmax()]
        st.metric("Top Gainer Today", top_gainer['coin_name'], f"+{top_gainer['pct_change']}%")
        
    with col3:
        avg_change = df['pct_change'].mean()
        st.metric("Market Average (24h)", f"{avg_change:.2f}%", delta_color="normal")

    with col4:
        st.metric("Assets Tracked", f"{len(df)} Coins")

    st.write("") # Spacer

    # 4. Visualizations
    tab1, tab2 = st.tabs(["📊 Performance Chart", "🔎 Raw Data"])

    with tab1:
        st.subheader("Price Change % by Asset")
        # Horizontal bar chart is more professional for many coins
        st.bar_chart(data=df.sort_values('pct_change', ascending=False), x="coin_name", y="pct_change", color="#1E88E5")

    with tab2:
        st.subheader("Consolidated Market Data")
        # Cleaning up the view
        display_df = df[['coin_name', 'price_usd', 'pct_change']].copy()
        display_df.columns = ['Asset', 'Price (USD)', 'Change (24h %)']
        st.dataframe(display_df.style.format({'Price (USD)': '{:,.2f}', 'Change (24h %)': '{:+.2f}%'}), use_container_width=True)

    # 5. Footer
    st.caption(f"Last automated update: {df['extracted_at'].iloc[0] if 'extracted_at' in df.columns else 'Today'}")