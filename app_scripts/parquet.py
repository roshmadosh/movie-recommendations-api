from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).parent.parent


##
# A function for creating parquet files from csv's.
#
# I already wrote logic for writing csv's, creating parquet files directly would require some
# refactoring on fetch.py, but I'm too lazy.
#
# @author: Hiroshi Nobuoka
##
def _generate_parquet(objs):
    for obj in objs:
        df = pd.read_csv(obj["url"], lineterminator='\n')
        table = pa.Table.from_pandas(df)
        pq.write_table(table, f'{ROOT}/app_data/{obj["fileName"]}', compression="gzip")


if __name__ == "__main__":
    csvs = [
        {
            "fileName": "details-5000.parquet",
            "url": f'{ROOT}/app_data/details-5000.csv'
        },
        {
            "fileName": "genre_list-5000.parquet",
            "url": f'{ROOT}/app_data/genre_list-5000.csv'
        }
   ]
    _generate_parquet(csvs)

