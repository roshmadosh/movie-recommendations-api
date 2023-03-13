import numpy as np
import pandas as pd
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.feature_extraction.text import TfidfVectorizer
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


TOKENS_FILENAME = "tokens-5000.parquet"
ROOT = Path(__file__).parent.parent


##
# I already wrote logic for writing csv's, creating parquet files directly would require some
# refactoring on fetch.py, but I'm too lazy.
##
def _csv_to_parquet(objs):
    for obj in objs:
        df = pd.read_csv(obj["url"], lineterminator='\n')
        table = pa.Table.from_pandas(df)
        pq.write_table(table, f'{ROOT}/app_data/{obj["fileName"]}', compression="gzip")


##
# Tokenized data saved locally as parquet file.
##
def _generate_tokens_parquet():
    print("Generating tokens parquet...")
    details_table = pq.read_table(f'{ROOT}/app_data/details-5000.parquet')
    details_df = details_table.to_pandas()
    details_df.replace(np.nan, '', regex=True, inplace=True)

    tfidf_vectorizer = TfidfVectorizer(analyzer='word', stop_words='english')

    # get the column values to vectorize
    titles = details_df['title'].tolist()
    overviews = details_df['overview'].tolist()

    # do the tfidf stuff
    titles_matrix = tfidf_vectorizer.fit_transform(titles)
    titles_tokens = tfidf_vectorizer.get_feature_names_out()

    overviews_matrix = tfidf_vectorizer.fit_transform(overviews)
    overviews_tokens = tfidf_vectorizer.get_feature_names_out()

    # create tokenized dataframes
    titles_df = pd.DataFrame(titles_matrix.toarray(), columns = titles_tokens).add_prefix("ti_")
    overviews_df = pd.DataFrame(overviews_matrix.toarray(), columns = overviews_tokens).add_prefix("ov_")

    # consolidate
    tokens_df = pd.concat([titles_df, overviews_df], axis=1)

    # save as parquet file
    table = pa.Table.from_pandas(tokens_df)
    loc = f'{ROOT}/app_data/{TOKENS_FILENAME}'
    print("Saving parquet to", loc)
    pq.write_table(table, loc, compression="gzip")


def save_to_azure(container_name, file_name, file_location):
    account_url = "https://movierecsdataset.blob.core.windows.net"
    default_credential = DefaultAzureCredential()

    blob_service_client = BlobServiceClient(account_url, credential=default_credential)

    if blob_service_client.get_container_client(container_name) is None:
        print("Creating new container...")
        blob_service_client.create_container(container_name)

    blob_client =  blob_service_client.get_blob_client(container=container_name, blob=file_name)

    print("Uploading file to Azure storage as \n\t", file_name)

    with open(file=file_location, mode="rb") as data:
        blob_client.upload_blob(data, overwrite=True)


if __name__ == "__main__":
    objs = [
        {"fileName": "details-5000.parquet", "url": f'{ROOT}/app_data/details-5000.csv'},
        {"fileName": "genre_list-5000.parquet", "url": f'{ROOT}/app_data/genre_list-5000.csv'},
    ]
    _csv_to_parquet(objs)
    _generate_tokens_parquet()

    # save parquets to Azure
    for obj in objs:
        save_to_azure("datasets", obj["fileName"], obj["url"])

    save_to_azure("datasets", TOKENS_FILENAME, f'{ROOT}/app_data/{TOKENS_FILENAME}')