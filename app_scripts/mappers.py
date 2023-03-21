
def movie_list_to_dict(movieList):
    return [
        {
            "id": movie[0],
            "title": movie[1],
            "overview": movie[2],
            "genres": movie[3],
            "popularity": movie[4],
            "posterPath": movie[5]
        } for movie in movieList
    ]