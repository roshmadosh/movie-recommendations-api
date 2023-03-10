import requests
import json
from pathlib import Path
import os
from dotenv import load_dotenv

##
# Script for fetching movie data from the TMDB API and saving it to a file.
#
# Author: Hiroshi Nobuoka
# Date: 03/09/2023
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
def _get_movie_ids(count):
    print('Getting movie IDs from', MOVIE_ID_FILE)
    ids = []
    with open(f'{ROOT}/app_data/{MOVIE_ID_FILE}', 'r') as id_file:
        for line in id_file:
            line = line.rstrip()
            if line:
                obj = json.loads(line)
                ids.append(obj['id'])

    return ids[:count]

'''
Generates two files:
    - "details.csv": each row contains meta-data for a movie 
    - "genre_list.csv": each row contains a movie's categorized genre(s) in a dummified manner
'''
def fetch_movie_data():
    # Initialize data that will be added to details.csv
    movie_details_cols = ['id', 'title', 'overview', 'popularity', 'poster_path']
    movie_details_header = ','.join(movie_details_cols)
    movie_details = [movie_details_header]

    # Initialize data that will be added to genre_list.csv
    genre_map = _fetch_genre_map()
    genre_cols = ['id', 'title']
    for name in genre_map.values():
        genre_cols.append(name)
    genre_header = ','.join(genre_cols)
    movie_genre_lists = [genre_header]

    ids = _get_movie_ids(COUNT)
    for movie_id in ids:
        print("Fetching movie with id", movie_id)
        url = f'{API_HOST}/movie/{movie_id}?api_key={API_KEY}'
        req = requests.get(url)
        obj = req.json()

        # initialize row for each file
        detail_vals = [str(obj['id']), obj['title']]
        genre_vals = [str(obj['id']), obj['title']]

        # append to details row
        for col in movie_details_cols:
            detail_vals.append(str(obj[col]))

        movie_details.append(','.join(detail_vals))

        # append to genre_list row
        for genre_id in genre_map.keys():
            if genre_id in [genre['id'] for genre in obj['genres']]:
                genre_vals.append('1')
            else:
                genre_vals.append('0')

        movie_genre_lists.append(','.join(genre_vals))

    # write to files
    with open(f'{ROOT}/app_data/details-{COUNT}.csv', 'w+') as f:
        for movie in movie_details:
            f.write(str(movie) + "\n")

    with open(f'{ROOT}/app_data/genrelist-{COUNT}.csv', 'w+') as f:
        for movie in movie_genre_lists:
            f.write(str(movie) + "\n")


# entry point
if __name__ == "__main__":
    fetch_movie_data()