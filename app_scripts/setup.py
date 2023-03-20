import pandas as pd
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from app_scripts.fetch_ms import fetch_details
from sklearn.feature_extraction.text import TfidfVectorizer
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


TOKENS_FILENAME = "tokens.parquet"
ROOT = Path(__file__).parent.parent


##
# Tokenized data saved locally as parquet file.
##
def _generate_all_tokens_parquet(count):
    details_df = fetch_details(count)
    print("Details dataframe generated. Commencing string tokenization...")
    tfidf_vectorizer = TfidfVectorizer(analyzer='word', stop_words='english', min_df=2)

    titles_matrix = tfidf_vectorizer.fit_transform(details_df["title"].tolist())
    titles_df = pd.DataFrame(titles_matrix.toarray(), columns=tfidf_vectorizer.get_feature_names_out())
    print("Titles dataframe successfully created. Dataframe shape:")
    print(titles_df.shape)

    overview_matrix = tfidf_vectorizer.fit_transform(details_df["overview"].tolist())
    overview_df = pd.DataFrame(overview_matrix.toarray(), columns=tfidf_vectorizer.get_feature_names_out())
    print("Overviews dataframe successfully created. Dataframe shape:")
    print(overview_df.shape)

    # Memory intensive. For > 20k movies, may have to write dataframes to disk first.
    print("Attempting to concatenate dataframes...")
    overview_df.join(titles_df, lsuffix="_ov", rsuffix="_ti")
    print("Concantenation successfull!")
    print(overview_df.shape)

    print("Generating parquet table...")
    table = pa.Table.from_pandas(overview_df)
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
    COUNT = 20000
    _generate_all_tokens_parquet(COUNT)
    #
    # # save parquets to Azure
    # for obj in objs:
    #     save_to_azure("datasets", obj["fileName"], obj["url"])

    # save_to_azure("datasets", TOKENS_FILENAME, f'{ROOT}/app_data/{TOKENS_FILENAME}')