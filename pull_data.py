import duckdb
import requests
import pandas as pd 
from sodapy import Socrata
from ratelimit import limits, sleep_and_retry
import time 
import logging 
import os
from dotenv import load_dotenv
load_dotenv()  

logging.basicConfig(
    level=logging.DEBUG,  
    filename='datapipeline.log',
    filemode="w",
    format='%(asctime)s - %(levelname)s - %(message)s')

URL="data.cityofnewyork.us"
timeout=30
dataset_id="76xm-jjuj"
APP_TOKEN = os.getenv("APP_TOKEN")

client = Socrata(URL,app_token=APP_TOKEN,timeout=timeout)

def _fetch_data_with_retry(offset, limit, where,dataset_id=dataset_id):
    """
    
    """
    retry_count=0
    success=False
    results=None

    while retry_count < 3:
        try:
            
            results = client.get(dataset_id,where=where,offset=offset,limit=limit)
            logging.info(f"Got {len(results)} results")
            success=True
            logging.info(f"Breaking from try loop")
            break
        except  Exception as e:
            retry_count += 1
            time.sleep(30)
            logging.error("⚠️ Something went wrong: %s", e)
    return success, results
    

def pull_data():
    """
    
    """
    offset=0
    limit=50000
    rows=[]

    while True:
        success,results = _fetch_data_with_retry(offset=offset, limit=limit, 
                                                 where= "INCIDENT_DATETIME between "
                                                    "'2025-05-01T00:00:00.000' and "
                                                    "'2025-12-31T23:59:59.999'")
        if not success:
            temp_df = pd.DataFrame(rows)
            logging.warning(f"⚠️ WARNING: Failed after 3 retries. Returning {len(rows)} partial records %s")
            return temp_df
        if results:
            logging.info(f"Processing {len(results)}")
            offset += limit
            for x in results:
                rows.append(x)
            logging.info(f"Rows:{len(rows)}")
            logging.info(f'Offset:{offset}')
            continue
        else:
            logging.info(f'No more results, breaking')
            break

    df=pd.DataFrame(rows)
    return df





