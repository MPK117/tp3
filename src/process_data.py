import pandas as pd
from typing import List
import os
import glob
from pathlib import Path
import json

col_date: str = "date_heure"
col_donnees: str = "consommation"
cols: List[str] = [col_date, col_donnees]
fic_export_data: str = "data/interim/data.csv"


def load_data():
    list_fic: list[str] = [Path(e) for e in glob.glob("data/raw/*json")]
    list_df: list[pd.DataFrame] = []
    for p in list_fic:
        with open(p, "r") as f:
            dict_data: dict = json.load(f)
            df: pd.DataFrame = pd.DataFrame.from_dict(dict_data.get("results"))
            list_df.append(df)

    df: pd.DataFrame = pd.concat(list_df, ignore_index=True)
    return df


def format_data(df: pd.DataFrame):
    # Typage
    df[col_date] = pd.to_datetime(df[col_date])
    # Ordre
    df = df.sort_values(col_date)
    # Filtrage des colonnes
    df = df[cols]
    # Dédoublonnage
    df = df.drop_duplicates()
    return df


def calculate_daily_average(df: pd.DataFrame):
    df["day_of_week"] = df[col_date].dt.day_name()
    daily_avg = df.groupby("day_of_week")[col_donnees].mean().reset_index()
    return daily_avg


def calculate_daily_min_max(df: pd.DataFrame):
    daily_min_max = df.groupby(df[col_date].dt.date)[col_donnees].agg(['min', 'max']).reset_index()
    daily_min_max.columns = ['date', 'min_consumption', 'max_consumption']
    return daily_min_max


def export_data(df: pd.DataFrame, filename: str):
    os.makedirs("data/interim/", exist_ok=True)
    df.to_csv(filename, index=False)


def main_process():
    df: pd.DataFrame = load_data()
    df = format_data(df)
    daily_avg = calculate_daily_average(df)
    daily_min_max = calculate_daily_min_max(df)
    export_data(df, fic_export_data)
    return daily_avg, daily_min_max


if __name__ == "__main__":
    main_process()
