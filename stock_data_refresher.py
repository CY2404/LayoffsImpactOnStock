import os
import json

import yfinance as yf
import pandas as pd
from datetime import date

from azure_accessor import file_downloader, file_uploader
from constants import (
    STOCK_START_DATE, STOCK_CONTAINER_NAME, OVERVIEW_FILE_LOC, OVERVIEW_FILE_LOCATION
)

CHANGE_PCT_NAME = "Change_Percentage"

STOCK_OUTPUT_LOC = "/Users/qwu194/Documents/playground/playgroud_azure/test_file/all_stock_json.json"

def _overview_checker(overview_path: str):
    return os.path.exists(overview_path)

def _write_json_to_file(output_loc: str, json_data: list):
    with open(output_loc, "w") as output:
        json.dump(json_data, output)

def _get_list_of_tickers():
    if not _overview_checker(OVERVIEW_FILE_LOCATION):
        file_downloader(
            download_file_path=OVERVIEW_FILE_LOCATION,
            container_name=STOCK_CONTAINER_NAME,
            blob_name=OVERVIEW_FILE_LOC,
        )

    company_overview_df = pd.read_csv(OVERVIEW_FILE_LOCATION)
    company_overview_df = company_overview_df.iloc[: 30, : 6]

    overview_json = company_overview_df.to_json(orient='records')

    json_loc = f"{OVERVIEW_FILE_LOCATION[:-3]}json"
    if not _overview_checker(json_loc):
        _write_json_to_file(json_loc, overview_json)
        file_uploader(
            upload_file_path=json_loc,
            container_name=STOCK_CONTAINER_NAME,
            blob_name="stock_overview_json.json",
        )

    return company_overview_df["Ticker"]


def get_company_stock_data(ticker: str, start_date: str, end_date: str) -> str:
    """
        Ticker: str, i.e. GOOG, AMZN
        start_date: str, "2020-01-01"
        end_date:str , today

        output: file path
    """
    print(f"Getting fresh data for {ticker} at date {end_date}.")
    stock_df = yf.download(ticker, start=start_date, end=end_date)
    # Calculate pct change
    stock_df[CHANGE_PCT_NAME] = (
        stock_df[[stock_df.columns[0], stock_df.columns[3]]].pct_change(axis=1)[stock_df.columns[3]] * 100
    )
    # Groom through df
    stock_df = stock_df.drop(stock_df.columns[4], axis=1)
    stock_df['date'] = pd.to_datetime(stock_df.index).strftime('%Y-%m-%d')
    stock_df['date'] = stock_df['date'].astype(str)
    stock_df['ticker'] = ticker
    stock_df.to_csv(f"/Users/qwu194/Documents/playground/playgroud_azure/test_file/{ticker}.csv", index=False)
    # json_output = stock_df.to_json(orient='records')
    # return json_output

def main():
    """
        Main data refresher
    """
    current_date = date.today()
    ongoing_json = []
    _get_list_of_tickers()
    for ticker in _get_list_of_tickers():
        print(ticker)
        ticker_json = get_company_stock_data(
            ticker=ticker, 
            start_date=current_date-date.timedelta(day=1), 
            end_date=current_date,
        )
        ongoing_json.append(
            {"Stocks": ticker_json}
        )
    
    _write_json_to_file(STOCK_OUTPUT_LOC, ongoing_json)
    file_uploader(
        upload_file_path=STOCK_OUTPUT_LOC,
        container_name=STOCK_CONTAINER_NAME,
        blob_name=f"all_stock_json.json",
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        print("Exception:")
        print(ex)
