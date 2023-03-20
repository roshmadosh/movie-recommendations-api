import requests
import json
import pandas as pd
import numpy as np

PATH = 'http://localhost:8083/api/v1/ml/assets/details'

def fetch_details(count):
    print("Fetching movie details...")
    details_resp = requests.get(f'{PATH}?count={count}')
    details_json = json.loads(details_resp.content)

    details_df = pd.DataFrame(details_json)
    details_df.replace(np.nan, '', regex=True, inplace=True)

    return details_df