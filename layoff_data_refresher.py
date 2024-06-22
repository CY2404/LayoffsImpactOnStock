import os
import re

import yfinance as yf
import pandas as pd
from datetime import date, timedelta

from azure_accessor import file_uploader
from constants import (
    LAYOFF_CONTAINER_NAME, OVERVIEW_FILE_LOCATION
)


NEWEST_LAYOFF_DATA = "/Users/qwu194/Documents/playground/playgroud_azure/test_file/WARN_db_layoff_newest.csv"
NEWEST_LAYOFF_DATA_PARSED = "/Users/qwu194/Documents/playground/playgroud_azure/test_file/new_layoff_event.csv"

NEW_LAYOFF_COLUMN = ['Date', 'Type', 'Company', 'CUSIP']

def get_company_names():
    company_overview_df = pd.read_csv(OVERVIEW_FILE_LOCATION)
    company_overview_df = company_overview_df.iloc[: 30, : 6]

    parsed_companies = {}
    for company_str, cusip in zip(company_overview_df['Company'], company_overview_df['CUSIP']):
        parsed_c = re.findall(r'[^.\s]+', company_str)[0]
        parsed_companies[parsed_c] = [company_str,cusip]

    return parsed_companies

def layoff_data_parser(current_date:str = date.today()) -> str:
    print(f"Refreshing layoff data")

    yesterday = pd.Timestamp(current_date - timedelta(days =1))

    layoff_df = pd.read_csv(NEWEST_LAYOFF_DATA)

    layoff_df = layoff_df[layoff_df["Effective Date"].notna() & layoff_df["Company"].notna()]

    layoff_df['Date'] = pd.to_datetime(layoff_df['WARN Received Date'], format='%Y-%m-%d', errors="coerce")
    layoff_df['Type'] = layoff_df['Closure/Layoff']

    layoff_df = layoff_df[layoff_df.Date > pd.Timestamp(2022,1,1)]
    
    new_layoffs = pd.DataFrame(columns=NEW_LAYOFF_COLUMN)

    comp_dict = get_company_names()
    print(comp_dict)
    for company in comp_dict.keys():
        if company == 'Amazon':
            data_parser = layoff_df[layoff_df['Company'].str.contains(company)]
            if not data_parser.empty:
                new_df = data_parser[['Company', 'Date', 'Type']].copy()
                new_df['Company'] = comp_dict[company][0]
                new_df['CUSIP'] = comp_dict[company][1]
                new_layoffs = pd.concat([new_layoffs, new_df], ignore_index=True)
    
    new_layoffs.to_csv(NEWEST_LAYOFF_DATA_PARSED, index=False)


def main():
    """
        Main data refresher
    """
    layoff_data_parser()
    file_uploader(
        upload_file_path=NEWEST_LAYOFF_DATA_PARSED,
        container_name=LAYOFF_CONTAINER_NAME,
        blob_name=f"new_layoff_events.csv",
    )

if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        print("Exception:")
        print(ex)
