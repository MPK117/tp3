import streamlit as st
import pandas as pd
import plotly.express as px
from src.fetch_data import load_data_from_lag_to_today
from src.process_data import col_date, col_donnees, main_process, fic_export_data, calculate_daily_average, calculate_daily_min_max
import logging
import os
import glob
import time
import requests
from dotenv import load_dotenv
 
load_dotenv()
from schedule import every, repeat
import schedule
LENGTH_DATA: int = 0

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
    daily_avg, daily_min_max = main_process()
    data = pd.read_csv(fic_export_data, parse_dates=[col_date])
    return data, daily_avg, daily_min_max

df, daily_avg, daily_min_max = load_data(LAG_N_DAYS)

st.subheader("Line Chart of Numerical Data Over Time")
numerical_column = col_donnees
fig = px.line(df, x=col_date, y=col_donnees, title="Consommation en fonction du temps")
st.plotly_chart(fig)

st.subheader("Average Consumption per Day of the Week")

# Create a bar chart for daily average consumption
fig_bar = px.bar(daily_avg, x='day_of_week', y=col_donnees, title='Moyenne de la consommation par jour de la semaine')
st.plotly_chart(fig_bar)

st.subheader("Daily Minimum and Maximum Consumption over the Last 7 Days")

# Display daily minimum and maximum consumption as numbers
st.write("Consommation quotidienne minimale et maximale sur les 7 derniers jours :")
st.write(daily_min_max)

while True:
    schedule.run_pending()
    df: pd.DataFrame = pd.read_csv(fic_export_data, parse_dates=[col_date])
    if len(df) > LENGTH_DATA:
        LENGTH_DATA = len(df)
        logging.info(f"Nb points de mesure: {LENGTH_DATA}")
        st.toast("Nouvelles données disponibles", icon="🎉")
    requests.post(
        os.environ["BLOWERIO_URL"] + "/messages",
        data={"to": "+33676XXXXXX", "message": "Test du SMS"},
    )