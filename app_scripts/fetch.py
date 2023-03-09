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
API_HOST = 'https://api.themoviedb.org/3/movie/'
ROOT = Path(__file__).parent.parent

# file obtained from http://files.tmdb.org/p/exports/movie_ids_03_08_2023.json.gz
MOVIE_ID_FILE = 'movie_ids_03_08_2023.json'
COUNT = 1000

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

def fetch_movie_data():
    movies = []
    ids = _get_movie_ids(COUNT)
    for movie_id in ids:
        print("Fetching movie with id", movie_id)
        url = f'{API_HOST}/{movie_id}?api_key={API_KEY}'
        req = requests.get(url)
        movies.append(req.json())

    with open(f'{ROOT}/app_data/data-{COUNT}.json', 'w+') as f:
        for movie in movies:
            f.write(str(movie) + "\n")


if __name__ == "__main__":
    fetch_movie_data()