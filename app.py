import streamlit as st
import pandas as pd
import plotly.express as px
from src.fetch_data import load_data_from_lag_to_today
from src.process_data import col_date, col_donnees, main_process, fic_export_data, calculate_daily_average
import logging
import os
import glob

logging.basicConfig(level=logging.INFO)

LAG_N_DAYS: int = 7

os.makedirs("data/raw/", exist_ok=True)
os.makedirs("data/interim/", exist_ok=True)

for file_path in glob.glob("data/raw/*json"):
    try:
        os.remove(file_path)
    except FileNotFoundError as e:
        logging.warning(e)

st.title("Data Visualization App")

@st.cache_data(ttl=15 * 60)
def load_data(lag_days: int):
    load_data_from_lag_to_today(lag_days)
    daily_avg = main_process()
    data = pd.read_csv(fic_export_data, parse_dates=[col_date])
    return data, daily_avg

df, daily_avg = load_data(LAG_N_DAYS)

st.subheader("Line Chart of Numerical Data Over Time")
numerical_column = col_donnees
fig = px.line(df, x=col_date, y=col_donnees, title="Consommation en fonction du temps")
st.plotly_chart(fig)

st.subheader("Average Consumption per Day of the Week")
st.write(daily_avg)
