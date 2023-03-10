# Microservice API for a Machine Learning Model that Gives Movie Recommendations
---
Train data obtained using [The Movie Database](https://developers.themoviedb.org/3/getting-started) API.  

To run locally, `app_scripts/fetch.py` generates your data files.

Recommendation model created using `scikit-learn`'s `TfidfVectorizer` to tokenize movie title and summary, then combining the cosine similarities of the tokens and the genre categories to produce a single, pairwise similarity score.