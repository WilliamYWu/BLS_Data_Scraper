# **BLS CPI Data Scraper**

Description: This will pull Urban Consumer CPI Data from BLS using its json based API.
Below are some references:

* [FUll ID CREATION](https://www.bls.gov/help/hlpforma.htm#id=CU)
* [AREA CODES](https://download.bls.gov/pub/time.series/cu/cu.area)
* [ITEM CODES](https://download.bls.gov/pub/time.series/cu/cu.item)
* [MANUAL DOWNLOAD](https://www.bls.gov/cpi/data.htm)
* [API INFORMATION](https://www.bls.gov/developers/api_faqs.htm#register1)


# Section 1: Setup

1. Packages
2. Directories
3. Helper Functions
4. Setup Functions


## Section 1.1 Packages

```python
from datetime import datetime
import logging
import os
import logging.handlers

import time
import requests
import json
import re
import csv
import pandas as pd
```

## Section 1.2 Directories

```python
# NOTE: All directories the program used should be included as a global variable here
MAIN_DIR =  "D:\\Code\\PYTHON\\BLS_SCRAPER\\"
DATA_DIR = MAIN_DIR + f"Data"
# NOTE: Automatic Log Folder directory creation based on date.
# NOTE: The file iteself is created based on the time. 
LOG_DIR = MAIN_DIR + f"Log\\{datetime.now().strftime('%Y%m%d')}\\" 
LOG_FILE = LOG_DIR + f"Log_{datetime.now().strftime('%H%M%S')}.log"
```

## Section 1.3 Helper Functions

```python
def directory_setup(dir_list):
    '''
    DESCRIPTION -> If the directory does not exist it will create it
    '''
    for directory in dir_list:
        if not os.path.exists(directory):
            os.makedirs(directory)

def logging_setup():
    '''
    DESCRIPTION -> Setups the logging file for code
    '''
    try:
      handler = logging.handlers.WatchedFileHandler(os.environ.get("LOGFILE", LOG_FILE))
      formatter = logging.Formatter(fmt="%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
      handler.setFormatter(formatter)
      logging.getLogger().handlers.clear()
      root = logging.getLogger()
      root.setLevel(os.environ.get("LOGLEVEL", "INFO"))
      root.addHandler(handler)
      logging.propogate = False
      logging.info("Log File was created successfully.")
    except Exception as e:
        exit
```

```python
# NOTE: All steps regrading setup should be completed here
DIR_LIST = [MAIN_DIR, LOG_DIR, DATA_DIR]
directory_setup(DIR_LIST)
logging_setup()
```

# Section 2: BLS Scraping Class

Description: This is the main method used to scrape the information off of BLS.

Contains the methods
* get_data
* process_data

```python
'''
------------------------------------------------------------------------------------------------------------
-----------------------------------------------DESCRIPTION--------------------------------------------------
------------------------------------------------------------------------------------------------------------

Passes the BLS json request and gets the data. 
Afterwards it processes the data and enriches the data with some additional information about area and item names.

------------------------------------------------------------------------------------------------------------
-----------------------------------------------PARAMETERS---------------------------------------------------
------------------------------------------------------------------------------------------------------------
api_key -> API_KEY for BLS data queries
out_file -> Location for Data to be outputted
series_id -> All the series of CPI data that you want
start_year -> Query start range
end_year -> Query end range
area_df -> Dataframe containing information on metro area codes and names
item_df -> Dataframe containing information on item codes and names

'''
class bls_data_scraper:

    def __init__(self, api_key, out_file, series_id, start_year, end_year, area_df, item_df):
        headers = {"Content-type": "application/json"}
        parameters = json.dumps({
                                "seriesid":series_id, 
                                "startyear":start_year, 
                                "endyear":end_year, 
                                "registrationkey":api_key
                                })
        self.area_df = area_df
        self.item_df = item_df
        
        # Requests the data from BLS
        json_data = self.get_data(headers, parameters)
        # Processes the data from BLS
        df_data = self.process_data(json_data, area_df, item_df)
        # Converts the data to an array to write -> Need to do this so that we have a single header
        list_df_data = df_data.values.tolist()

        # WARNING: One issue if you kill the run and start again it will continue to append to the same file

        # Writes the cleaned up data into the specified out_file
        with open(out_file , "a") as file:
            headers = ["ID","area_name","item_name","year","period","periodName","latest","value","footnotes"]
            writer = csv.writer(file, delimiter=',', lineterminator='\n')
            if os.stat(out_file).st_size==0:
                writer.writerow(headers)
            for row in list_df_data:
                writer.writerow(row)


    def get_data(self, headers, parameters):
        '''
        DESCRIPTION -> Posts the url and we get the data back in a json format

        PARAM 1 -> headers -> self.header a BLS API requirement
        PARAM 2 -> parameters -> The data specification that you plan on querying
        '''
        post = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=parameters, headers=headers)
        json_data = json.loads(post.text)
        return json_data
    

    def process_data(self, json_data, area_df, item_df):
        '''
        DESCRIPTION -> Cleans and enriches the JSON data that we just processed

        PARAM 1 -> json_data -> The raw JSON data we pulled from BLS
        PARAM 2 -> area_df -> The area code and name information
        PARAM 3 -> item_df -> The item code and name information
        '''

        # NOTE: A lot of the data is stored inside multi-layed dictionaries/lists
        # All the information is stored under a three layer depth
        df = pd.json_normalize(json_data, record_path=["Results", "series", "data"], meta=[["Results", "series", "seriesID"]])
        df.rename(columns = {"Results.series.seriesID":"ID"}, inplace = True)

        # Parsing out the area_code and item_code from the entirety of the ID that we generated
        df["area_code"] = df["ID"].apply(lambda x: x[4:8])
        df["item_code"] = df["ID"].apply(lambda x: x[8:])

        # Enriching the data here
        df = pd.merge(df, area_df, how="left", on="area_code")
        df = pd.merge(df, item_df, how="left", on="item_code")
        df.drop(columns=["area_code", "item_code"], inplace=True)

        # rearrange column ordering
        name_list = df.columns.tolist()
        name_list = name_list[-3:-2] + name_list[-2:-1] + name_list[-1:] + name_list[:-3]
        df = df[name_list]
        
        return df
```

# Section 3: BLS Area and Item Information Scraper

1. bls_code_scraper function 
2. Get and Filter the Information
3. Generate List of BLS Codes from Filtered Data


# Section 3.1 bls_code_scraper function

```python
def bls_code_scraper(url, file_dir):
    '''
    DESCRIPTION -> Scrapes textual information off of BLS links

    PARAM 1 -> file_dir -> Where you want to write the raw_information
    '''

    response = requests.get(url)
    content = response.content.decode("utf-8")
    area_content = csv.reader(content.splitlines(), delimiter="\t")
    area_list = list(area_content)

    # n is the number of elements that we want to remove from the back of the list
    n = 3
    # Writing the uncleaned relavant information into file 
    with open(file_dir, "w") as file:
        writer = csv.writer(file)
        for row in area_list:
            row = row[: len(row) - n]
            writer.writerow(row)
    file.close()
```

## Section 3.2 Get and Filter the Information

```python
# Scraping the area code information
bls_code_scraper("https://download.bls.gov/pub/time.series/cu/cu.area", f"{DATA_DIR}\\raw_area.csv")
# Scraping the item code information
bls_code_scraper("https://download.bls.gov/pub/time.series/cu/cu.item", f"{DATA_DIR}\\raw_item.csv")
```

```python
area_df = pd.read_csv(f"{DATA_DIR}\\raw_area.csv")

# Filter for S area_code, since we only want metro area codes
area_df["code_check"] = area_df["area_code"].apply(lambda x: "Pass" if re.match("S[0-9]{3}", x) else "Fail")
area_df = area_df[area_df["code_check"] == "Pass"]

# Remove the area_names that have Size Class A in their name. Those are irrelavent.
area_df["name_check"] = area_df["area_name"].apply(lambda x: "Pass" if "Size Class A" not in x else "Fail")
area_df = area_df[area_df["name_check"] == "Pass"]

area_df.drop(columns=["code_check", "name_check"], inplace=True)
area_df.to_csv(f"{DATA_DIR}\\clean_area.csv", index=False)


item_df = pd.read_csv(f"{DATA_DIR}\\raw_item.csv")
# Only include item_codes that have a shorter length than 4 since those are the more aggregated values.
item_df["code_check"] = item_df["item_code"].apply(lambda x: "Pass" if len(x)<4 else "Fail")
item_df = item_df[item_df["code_check"] == "Pass"]

# If the item_name contains All Items we remove since those are too aggregated
item_df["name_check"] = item_df["item_name"].apply(lambda x: "Pass" if not re.match("All items [a-zA-Z\s,]*",x) else "Fail")
item_df = item_df[item_df["name_check"] == "Pass"]

item_df.drop(columns=["code_check", "name_check"], inplace=True)
item_df.to_csv(f"{DATA_DIR}\\clean_item.csv", index=False)
```

## Section 3.3 Generate List of BLS Codes from Filtered Data

```python
"""
Here is a sample of how the BLS creates the unique identifier. You can find more information on it here (https://www.bls.gov/help/hlpforma.htm#id=CU).

	Series ID    CUUR0000SA0L1E
	Positions       Value           Field Name
	1-2             CU              Prefix
	3               U               Not Seasonal Adjustment Code
	4               R               Periodicity Code
	5-8             0000            Area Code
	9               S               Base Code
	10-16           A0L1E           Item Code
"""
code_list = []
prefix = "CU"
# This dataset does not have any seasonally adjusted data
seasonality = "U"
# R stands for monthly
periodicity = "R"
for area in area_df["area_code"]:
    for item in item_df["item_code"]:
        code_list.append(f"{prefix}{seasonality}{periodicity}{str(area)}{str(item)}")
logging.info(f"Total Unique Codes: {len(code_list)}")
```

# Section 4: Main

```python
'''
Limit of 500 calls per key.
Can make 50 calls every query.
'''

api_key = "API KEY GOES HERE"
start_year = 2019
end_year = 2022
for x in range(0, len(code_list), 50):
    code_chunk = code_list[x:x+50]
    bls_data_scraper(api_key, f"{DATA_DIR}\\bls_data.csv", code_chunk, start_year, end_year, area_df, item_df)
    time.sleep(2)
print("Done")
```
