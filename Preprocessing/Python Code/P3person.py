from faker import Faker
import pandas as pd
from datetime import datetime
import random

fake = Faker()

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



tech_job_titles = [
    'Software Engineer', 'Data Scientist', 'Product Manager',
    'Systems Analyst', 'Web Developer', 'IT Specialist',
    'Tech Support Specialist', 'Network Engineer', 'UX Designer',
    'Security Analyst', 'Database Administrator', 'DevOps Engineer'
]

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


companies = []
for line in companies_info.strip().split("\n"):
    parts = line.split()
    company_id = parts[0]
    company_name = " ".join(parts[1:])
    companies.append({"CompanyID": company_id, "CompanyName": company_name})

def random_dob(age_start, age_end):
    earliest_year = 1970
    latest_year = 1995
    year = random.randint(earliest_year, latest_year)
    month = random.randint(1, 12)
    if month in [1, 3, 5, 7, 8, 10, 12]:
        day = random.randint(1, 31)
    elif month == 2:
        if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):
            day = random.randint(1, 29)
        else:
            day = random.randint(1, 28)
    else:
        day = random.randint(1, 30)
    dob = datetime(year, month, day)
    return dob.strftime('%Y-%m-%d')

def random_hire_date():
    end_date = datetime(year=2022, month=12, day=31)
    return fake.date_between(start_date='-10y', end_date=end_date)


def random_layoff_date(hire_date):
    if random.choice([True, False]):
        return fake.date_between(start_date=hire_date, end_date='today')
    else:
        return ""

# Generate person data for each company
person_data = []
for company in companies:
    company_id = company['CompanyID']
    company_layoff_date = layoff_dates.get(company_id) 
    for _ in range(20): 
        hire_date = random_hire_date()
        person_data.append({
            "PersonID": fake.unique.random_int(min=10000, max=99999),
            "EventID": fake.random_int(min=1000, max=9999),
            "FirstName": fake.first_name(),
            "LastName": fake.last_name(),
            "JobTitle": random.choice(tech_job_titles),  
            "DateOfBirth": random_dob(21, 60),
            "CompanyID": company_id,
            "HireDate": hire_date.isoformat(),
            "LayOffDate": company_layoff_date,  
        })


df_persons = pd.DataFrame(person_data)

csv_file_path = 'C:/Users/teyal/Downloads/P3.csv'


df_persons.to_csv('C:/Users/teyal/Downloads/P3.csv', index=False)

csv_file_path, df_persons.head() 
