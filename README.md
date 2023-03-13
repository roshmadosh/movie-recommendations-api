# Microservice API for a Machine Learning Model that Gives Movie Recommendations
---
Train data obtained using [The Movie Database](https://developers.themoviedb.org/3/getting-started) API.  

`app_scripts/fetch.py` fetches data from TMDB database and saves as CSVs.    

`app_scripts/setup.py` generates parquet files used by model at runtime and saves them to Azure Blob Storage. 

Recommendation model created using `scikit-learn`'s `TfidfVectorizer` to tokenize movie title and summary, then combining the cosine similarities of the tokens and the genre categories to produce a single, pairwise similarity score.