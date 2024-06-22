from faker import Faker
import pandas as pd
from datetime import datetime
import random

fake = Faker()

companies_info = """
6 Enphase Energy Inc
21 Etsy Inc
56 Unity Software Inc
91 Tripadvisor Inc
311 SecureWorks Corp
504 Uber Technologies Inc
530 TrueCar Inc
540 Chegg Inc
562 Reddit Inc
628 Lemonade Inc
688 Dropbox Inc
690 Vroom Inc
709 LYFT Inc
712 Buzzfeed Inc
721 Opendoor Technologies Inc
800 Lucid Group Inc
829 Amazon.com Inc
846 Meta Platforms Inc
895 Thoughtworks Holding Inc
953 Micron Technology Inc
1025 Zoom Video Communications Inc
1026 eBay Inc
1084 PayPal Holdings Inc
1171 Alphabet Inc Class C
1204 Microsoft Corp
1498 DoorDash Inc
1587 Cisco Systems Inc
2303 Netflix Inc
1086 Workday Inc
1172 Wayfair Inc
"""
layoff_dates = {
    "6": "2023-12-18",
    "21": "2023-12-13",
    "56": "2023-11-28",
    "91": "2023-11-12",
    "311": "2023-08-14",
    "504": "2023-06-21",
    "530": "2023-06-14",
    "540": "2023-06-12",
    "562": "2023-06-06",
    "628": "2023-05-16",
    "688": "2023-04-27",
    "690": "2023-04-27",
    "709": "2023-04-21",
    "712": "2023-04-20",
    "721": "2023-04-18",
    "800": "2023-03-28",
    "829": "2023-03-20",
    "846": "2023-03-14",
    "895": "2023-03-01", 
    "953": "2023-02-17",
    "1025": "2023-02-07",
    "1026": "2023-02-07",
    "1084": "2023-01-31",
    "1171": "2023-01-20",
    "1204": "2023-01-18",
    "1498": "2023-03-01",
    "1587": "2023-08-18",
    "2303": "2023-04-28",
    "1086": "2023-01-31",
    "1172": "2023-01-20",
}

layoff_types= {
    "6": "Restructuring",
    "21": "Downsizing",
    "56": "Cost Reduction",
    "91": "Automation",
    "311": "Restructuring",
    "504": "Cost Reduction",
    "530": "Outsourcing",
    "540": "Downsizing",
    "562": "Automation",
    "628": "Relocation",
    "688": "Restructuring",
    "690": "Restructuring",
    "709": "Automation",
    "712": "Cost Reduction",
    "721": "Outsourcing",
    "800": "Merger or Acquisition",
    "829": "Cost Reduction",
    "846": "Outsourcing",
    "895": "Automation",
    "953": "Automation",
    "1025": "Relocation",
    "1026": "Merger or Acquisition",
    "1084": "Cost Reduction",
    "1171": "Restructuring",
    "1204": "Downsizing",
    "1498": "Outsourcing",
    "1587": "Automation",
    "2303": "Merger or Acquisition",
    "1086": "Downsizing",
    "1172": "Outsourcing"
}
layoff_events_data = []
for company_id, layoff_date in layoff_dates.items():
    event_data = {
        "EventID": fake.unique.random_int(min=1000, max=9999),
        "Date": layoff_date,
        "Type": layoff_types[company_id], 
        "CompanyID": company_id
    }
    layoff_events_data.append(event_data)

df_layoff_events = pd.DataFrame(layoff_events_data)

print(df_layoff_events.head())

csv_file_path = 'C:/Users/teyal/Downloads/layoff_table.csv'

df_layoff_events.to_csv(csv_file_path, index=False)
