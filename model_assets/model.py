from sklearn.metrics.pairwise import linear_kernel
import dill as pickle
from pathlib import Path
import pyarrow.parquet as pq
import numpy as np

ROOT = Path(__file__).parent.parent

# Extract pickled dataframes. DFs were pickled in jupyter notebook, prevents having to create
# them at runtime.
dfs = {}
for name in ["details", "genre_list", "tokens"]:
    dfs[name] = pq.read_table(f'{ROOT}/app_data/{name}-5000.parquet').to_pandas()
dfs["details"].replace(np.nan, '', regex=True, inplace=True)

# pair-wise similarity scores between movie tokens
token_sim = linear_kernel(dfs["tokens"], dfs["tokens"])

# same for genre similarity
genres_only = dfs["genre_list"].iloc[:, 2:]
genre_sim = linear_kernel(genres_only, genres_only)

# how much genre similarity affects score
GENRE_SCALE = 0.25


def get_top_n(title, n):
    # find index of title
    row = dfs["details"].loc[dfs["details"]['title'] == title]

    if row.empty:
        return []

    index = row.index.tolist()[0]

    # get sim scores for title
    token_scores = list(enumerate(token_sim[index]))
    genre_scores = list(enumerate(genre_sim[index]))

    total_scores = []

    for i, token_score in token_scores:
        total_scores.append((i, token_score + genre_scores[i][1] * GENRE_SCALE))

    # Sort the movies based on the similarity scores
    top_scores = sorted(total_scores, key=lambda x: x[1], reverse=True)[1:n + 1]

    top_indices = [row[0] for row in top_scores]

    top_titles = dfs["details"].iloc[top_indices, 1]
    return top_titles.to_numpy().tolist()
