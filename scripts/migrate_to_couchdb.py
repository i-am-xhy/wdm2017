import os

import psycopg2
import psycopg2.extensions

from couchdb import Server

from api.code.apiserver import execute

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)

conn = None
try:
    dbname = os.getenv('DBNAME', 'postgres')
    user = os.getenv('DBUSER', 'postgres')
    host = os.getenv('DBHOST', 'localhost')
    password = os.getenv('DBPASSWORD', 'Koekje123')
    conn = psycopg2.connect(dbname=dbname, user=user, host=host, password=password)
except:
    print("I am unable to connect to the database, exitting")
    exit(-1)

print('connecting to couch')
couch_server = Server()
print('connected to couch')
cdb = couch_server['movies']
print('couch db loaded')

basequery = '''SELECT m.idmovies, m.title, m.year, a.idactors, a.fname, a.lname, a.gender, ai.character
            FROM actors AS a
            JOIN acted_in AS ai ON a.idactors=ai.idactors
            JOIN movies AS m ON ai.idmovies=m.idmovies
            '''
# JOIN series AS s ON ai.idseries=s.idseries
# JOIN movies_keywords AS mk ON m.idmovies=mk.idmovies
# JOIN keywords AS k ON mk.idkeywords=k.idkeywords
# JOIN movies_genres AS mg ON m.idmovies=mg.idmovies
# JOIN genres AS g ON mg.idgenres=g.idgenres

movie_id_query = '''SELECT m.idmovies FROM movies AS m'''

movie_ids = [t[0] for t in execute(movie_id_query)]
movie_ids.sort()
# one-by-one solution
# movies = []
# no_movies = len(movie_ids)
# for i, movie_id in enumerate(movie_ids):
#     if i % 100 == 0:
#         print("{} movies processed out of {}".format(i, no_movies))
#     query = basequery + ' WHERE m.idmovies = ' + str(movie_id)
#     rows = execute(query)
#     if len(rows) > 0:
#         movie = {'movie_id': movie_id, 'title': rows[0][1], 'year': rows[0][2], 'actors': {}}
#         for row in rows:
#             actor_id = row[3]
#             if actor_id not in movie['actors']:
#                 movie['actors'][actor_id] = {'actor_id': row[3], 'first_name': row[4], 'last_name': row[5], 'gender': row[6], 'character': row[7]}
#         movie['actors'] = list(movie['actors'].values())
#         cdb[str(movie_id)] = movie
num_movies = len(movie_ids)
stepsize = 500
steps = num_movies / stepsize


def process_query(query):
    rows = execute(query)

    movies = {}
    for row in rows:
        movie_id = row[0]
        if movie_id not in movies:
            movies[movie_id] = {'movie_id': movie_id, 'title': row[1], 'year': row[2], 'actors': []}
        movies[movie_id]['actors'].append(
            {'actor_id': row[3], 'first_name': row[4], 'last_name': row[5], 'gender': row[6], 'character': row[7]})
        # long variant:
        # if movie_id not in movies:
        #     movies[movie_id] = {'movie_id': movie_id, 'title': row[1], 'year': row[2], 'actors': {}, 'series': [], 'keywords': [], 'genres': []}
        # movie = movies[movie_id]
        # actor_id = row[3]
        # if actor_id not in movie['actors']:
        #     movie['actors'][actor_id] = {'actor_id': row[3], 'first_name': row[4], 'last_name': row[5], 'gender': row[6], 'character': row[7]}
        # series = row[8]
        # if series not in movie['series']:
        #     movie['series'].append(series)
        # keyword = row[9]
        # if keyword not in movie['keywords']:
        #     movie['keywords'].append(keyword)
        # genre = row[10]
        # if genre not in movie['genres']:
        #     movie['genres'].append(genre)

    cdb.update(movies.values())


for i in range(0, steps):
    query = basequery + ' WHERE m.idmovies BETWEEN ' + str(movie_ids[i * stepsize]) + ' AND ' + str(
        movie_ids[(i + 1) * stepsize - 1])
    process_query(query)
    print('Completed step {} of {}'.format(i, steps))
query = basequery + ' WHERE m.idmovies BETWEEN ' + str(movie_ids[steps * stepsize]) + ' AND ' + str(movie_ids[-1])
process_query(query)
