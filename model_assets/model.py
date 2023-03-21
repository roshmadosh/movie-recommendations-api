from fastapi import HTTPException
from sklearn.metrics.pairwise import linear_kernel
from pathlib import Path
import pyarrow.parquet as pq
import numpy as np

from app_scripts.fetch_ms import get_details_as_dataframe, get_movie_genres_as_dataframe

ROOT = Path(__file__).parent.parent

# Read saved dataframes. DFs were saved using setup.py script.
dfs = {}
dfs["details"] = get_details_as_dataframe()
dfs["details"].replace(np.nan, '', regex=True, inplace=True)
dfs["genre_list"] = get_movie_genres_as_dataframe()

print("Generating tokens dataframe...")
dfs["tokens"] = pq.read_table(f'{ROOT}/app_data/tokens.parquet').to_pandas()

print("Calculating pair-wise similarity scores...")
# same for genre similarity
genres_only = dfs["genre_list"].iloc[:, 2:]
genre_sim = linear_kernel(genres_only, genres_only)

# pair-wise similarity scores between movie tokens
token_sim = linear_kernel(dfs["tokens"], dfs["tokens"])

# how much genre similarity affects score
GENRE_SCALE = 0.25

'''
Get the n-most recommended movie titles based on a collection of movie titles.

@input
    - titles: a list of title names (e.g. ["Toy Story", "Fight Club"]
    - n: the number of recommended movie titles to return as a list.
'''
def get_top_n(titles, n):
    token_scores = []
    genre_scores = []
    for title in titles:
        # find index of title
        row = dfs['details'].loc[dfs['details']['title'] == title]

        # movie title may not exist
        if row.empty:
            raise HTTPException(status_code=404, detail=f'Movie title "{title}" not found.')

        index = row.index.tolist()[0]
        token_score = token_sim[index]
        genre_score = genre_sim[index]

        token_scores.append(token_score)
        genre_scores.append(genre_score)

    # column-wise average of the movies
    avg_token_score = np.mean(token_scores, axis=0)
    avg_genre_score = np.mean(genre_scores, axis=0)

    # map each score to the ordered pair (index, score)
    enum_token_score = list(enumerate(avg_token_score))
    enum_genre_score = list(enumerate(avg_genre_score))

    total_scores = []

    for i, token_score in enum_token_score:
        total_scores.append((i, token_score + enum_genre_score[i][1] * GENRE_SCALE))

    # Sort the movies based on the similarity scores
    top_scores = sorted(total_scores,
        key=lambda x: x[1], reverse=True)[len(titles):len(titles) + n + 1]

    top_indices = [row[0] for row in top_scores]

    top_titles = dfs['details'].iloc[top_indices, 1]

    return top_titles.to_numpy().tolist()
