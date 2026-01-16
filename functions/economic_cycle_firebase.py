import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta
import requests
from urllib.parse import urlencode
from io import StringIO
from io import BytesIO
import zipfile

def calc_economy_status():
    '''
    # Base URL with the revision_date part to be replaced
    base_url = (
        "https://fred.stlouisfed.org/graph/fredgraph.csv?"
        "bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&"
        "height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&"
        "width=1320&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&"
        "id=BAMLH0A0HYM2&scale=left&cosd={}&coed={}&line_color=%234572a7&"
        "link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&"
        "fml=a&fq=Daily%2C%20Close&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&"
        "transformation=lin&vintage_date={}&revision_date={}&nd={}"
    )

    # Get today's date in YYYY-MM-DD format
    current_date = datetime.now().strftime("%Y-%m-%d")

    # Calculate the date 3652 days ago
    nd_date = (datetime.now() - timedelta(days=3652)).strftime("%Y-%m-%d")

    # Insert current date into the URL
    url_with_current_date = base_url.format(nd_date, current_date, current_date, current_date, nd_date)
    print(url_with_current_date)
    '''
    BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

    api_key = ""

    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=3652)).strftime("%Y-%m-%d")

    url = "https://api.stlouisfed.org/fred/series/observations"

    params = {
        "series_id": "BAMLH0A0HYM2",
        "api_key": api_key,
        "file_type": "csv",
        "observation_start": start_date,
        "observation_end": end_date,
    }

    # Use the Authorization header with Bearer token
    headers = {
        "Authorization": f"Bearer {api_key}"
    }

    # Fetch the CSV data from the updated URL
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()

    data = pd.DataFrame([])
    # Check content type
    content_type = response.headers.get("Content-Type")
    if content_type == "application/zip":
        #with open("fred_response.zip", "wb") as f:
        #    f.write(response.content)
        # Unzip in memory
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            csv_name = next(name for name in z.namelist() if name.lower().endswith(".csv"))
            with z.open(csv_name) as f:
                data = pd.read_csv(
                    f,
                    parse_dates=[0], 
                    na_values=".")
    else:
        # Plain CSV
        data = pd.read_csv(StringIO(response.text), parse_dates=[0], na_values=".")

    # Check if the request was successful
    if response.status_code == 200:
        data.to_csv("downloaded_data.csv", index=False)
    else:   
        print(f"Failed to fetch data. Status code: {response.status_code}")

    # Step 1: Clean and forward-fill column B values to fill missing data
    data[data.columns[1]] = pd.to_numeric(data[data.columns[1]], errors="coerce") # Convert the value column to float
    data["BAMLH0A0HYM2_filled"] = data[data.columns[1]].ffill()

    # Step 2: Calculate the median of the last 3650 rows in column C
    data["median_3650"] = (
        data["BAMLH0A0HYM2_filled"].rolling(window=3650, min_periods=1).median()
    )

    # Step 3: Calculate the average of values in column C for the next 90 to 97 days
    data["future_avg"] = data["BAMLH0A0HYM2_filled"].shift(90).rolling(window=8).mean() #faster approach

    # Step 4: Calculate the rolling average of the last 7 values
    data["rolling_avg_7"] = (
        data["BAMLH0A0HYM2_filled"].rolling(window=7, min_periods=1).mean()
    )

    # Step 5: Determine the status (RISING, FALLING, or N_A)
    def calculate_status(current, future_avg):
        if pd.isna(future_avg):
            return ""
        ratio = current / future_avg - 1
        if ratio > 0.05:
            return "RISING"
        elif ratio < -0.05:
            return "FALLING"
        else:
            return "N_A"

    data["status"] = data.apply(
        lambda row: calculate_status(row["rolling_avg_7"], row["future_avg"]), axis=1
    )

    # Step 6: Determine the position (ABOVE, BELOW, or N_A) relative to the median
    def calculate_position(current, median_3650):
        if pd.isna(median_3650):
            return ""
        ratio = current / median_3650 - 1
        if ratio > 0.05:
            return "ABOVE"
        elif ratio < -0.05:
            return "BELOW"
        else:
            return "N_A"

    data["position"] = data.apply(
        lambda row: calculate_position(row["BAMLH0A0HYM2_filled"], row["median_3650"]),
        axis=1,
    )

    # Step 7: Determine the final status based on columns F and G
    def calculate_final_status(row, prev_final_status):
        if row["status"] == "" or row["position"] == "":
            return prev_final_status
        elif row["status"] == "N_A" or row["position"] == "N_A":
            return prev_final_status
        elif row["status"] == "RISING" and row["position"] == "BELOW":
            return "OVERHEATING"
        elif row["status"] == "FALLING" and row["position"] == "BELOW":
            return "GROWTH"
        elif row["status"] == "RISING" and row["position"] == "ABOVE":
            return "RECESSION"
        elif row["status"] == "FALLING" and row["position"] == "ABOVE":
            return "RECOVERY"
        else:
            return ""

    # Iterate over rows to calculate the final status
    final_status = []
    prev_final_status = ""
    for _, row in data.iterrows():
        status = calculate_final_status(row, prev_final_status)
        final_status.append(status)
        prev_final_status = status

    data["final_status"] = final_status
    data = data.sort_values(by=data.columns[0], ascending=False).reset_index(drop=True)

    # Save the output to a new CSV file
    #data.to_csv('output_data.csv', index=False)

    # Step 1: Get the most recent final status
    latest_status = data.loc[data["final_status"] != "", "final_status"].iloc[0]

    # Step 2: Find the start of the current block of latest_status
    first_occurrence_index = (data["final_status"] != latest_status).idxmax() - 1
    first_occurrence = (
        data.iloc[first_occurrence_index]
        if first_occurrence_index >= 0
        else data.iloc[-1]
    )

    # Step 3: Find the preceding status before the block of latest_status
    preceding_status = (
        data.loc[first_occurrence_index + 1 :, "final_status"]
        .replace("", float("nan"))
        .dropna()
        .iloc[0]
        if first_occurrence_index + 1 < len(data)
        else "N/A"
    )

    # Print the current status, date since it started, and preceding status
    status = f'Current status: "{latest_status}", since: "{first_occurrence[data.columns[0]].date()}", before: "{preceding_status}"'
    return status


if __name__ == "__main__":
    print(calc_economy_status())
