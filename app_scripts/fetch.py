import concurrent.futures
import requests
import json
from pathlib import Path
import os
from dotenv import load_dotenv

##
# Script for fetching movie data from the TMDB API and saving it to a file.
#
# ** MAKE SURE TO DELETE THE CSV's BEFORE RUNNING! **
##

load_dotenv()
API_KEY = os.environ.get("tmdb-token")
API_HOST = 'https://api.themoviedb.org/3/'
ROOT = Path(__file__).parent.parent

# file obtained from http://files.tmdb.org/p/exports/movie_ids_03_08_2023.json.gz
MOVIE_ID_FILE = 'movie_ids_03_08_2023.json'
COUNT = 1000

'''
Generate a dictionary of the form: 

{ <genre-id>: <genre-name }

'''
def _fetch_genre_map():
    genre_map = {}
    print('Fetching genre list from TMDB API...')
    url = f'{API_HOST}/genre/movie/list?api_key={API_KEY}'
    req = requests.get(url)
    genres = req.json()['genres']
    for genre in genres:
        genre_map |= { genre['id']: genre['name']}

    return genre_map


'''
Obtains and returns valid movie ID's as a list.
'''
def _get_movie_ids(start):
    print('Getting movie IDs from', MOVIE_ID_FILE)
    ids = []
    with open(f'{ROOT}/app_data/{MOVIE_ID_FILE}', 'r') as id_file:
        for line in id_file:
            line = line.rstrip()
            if line:
                obj = json.loads(line)
                ids.append(obj['id'])

    return ids[start:start+COUNT]

'''
Generates two files:
    - "details.csv": each row contains meta-data for a movie 
    - "genre_list.csv": each row contains a movie's categorized genre(s) in a dummified manner
'''
def fetch_movie_data(start):
    # Initialize data that will be added to details.csv
    movie_details_cols = ['id', 'title', 'overview', 'popularity', 'poster_path']
    movie_details_header = ','.join(movie_details_cols)
    movie_details = [movie_details_header] if start == 0 else []

    # Initialize data that will be added to genre_list.csv
    genre_map = _fetch_genre_map()
    genre_cols = ['id', 'title']
    for name in genre_map.values():
        genre_cols.append(name)
    genre_header = ','.join(genre_cols)
    movie_genre_lists = [genre_header] if start == 0 else []

    ids = _get_movie_ids(start)
    for movie_id in ids:
        print("Fetching movie with id", movie_id)
        url = f'{API_HOST}/movie/{movie_id}?api_key={API_KEY}'
        req = requests.get(url)
        obj = req.json()

        # initialize row for each file
        detail_vals = []
        genre_vals = [str(obj['id']), obj['title'].replace(",", "").rstrip()]

        # append to details row
        for col in movie_details_cols:
            detail_vals.append(str(obj[col]).replace(",", "").rstrip())

        movie_details.append(','.join(detail_vals))

        # append to genre_list row
        for genre_id in genre_map.keys():
            if genre_id in [genre['id'] for genre in obj['genres']]:
                genre_vals.append('1')
            else:
                genre_vals.append('0')

        movie_genre_lists.append(','.join(genre_vals))

    # write to files
    with open(f'{ROOT}/app_data/details-{COUNT * THREADS}.csv', 'a+') as f:
        for movie in movie_details:
            f.write(str(movie) + "\n")

    with open(f'{ROOT}/app_data/genre_list-{COUNT * THREADS}.csv', 'a+') as f:
        for movie in movie_genre_lists:
            f.write(str(movie) + "\n")


# entry point
if __name__ == "__main__":
    THREADS = 5
    with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS) as executor:
        executor.map(fetch_movie_data, range(0, COUNT * (THREADS - 1) + 1, COUNT))

