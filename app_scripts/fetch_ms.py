import requests
import json
import pandas as pd
import numpy as np

def get_details_as_dataframe(count = 10000):
    print("Fetching movie details...")
    PATH = 'http://localhost:8083/api/v1/ml/assets/details'
    details_resp = requests.get(f'{PATH}?count={count}')
    details_json = json.loads(details_resp.content)

    details_df = pd.DataFrame(details_json)
    details_df.replace(np.nan, '', regex=True, inplace=True)

    print("Details fetched successfully!")
    return details_df

def get_movie_genres_as_dataframe():
    details_df = get_details_as_dataframe()
    genres = _fetch_genres()

    # setup for dataframe
    genre_map = {}
    genre_cols = ['id', 'title']
    for genre in genres:
        genre_map |= {genre['id']: genre['name']}
    for name in genre_map.values():
        genre_cols.append(name)
    movie_genre_list = []

    for ind, row in details_df.iterrows():
        genre_vals = [row['id'], row['title']]
        # append to genre_list row
        for genre_id in genre_map.keys():
            if genre_id in [genre['id'] for genre in row['genres']]:
                genre_vals.append('1')
            else:
                genre_vals.append('0')

        movie_genre_list.append(genre_vals)

    return pd.DataFrame(movie_genre_list, columns=genre_cols)


def _fetch_genres():
    print("Fetching genres...")
    PATH = 'http://localhost:8083/api/v1/ml/assets/genres'
    resp = requests.get(PATH)
    genres = resp.json()
    return genres
