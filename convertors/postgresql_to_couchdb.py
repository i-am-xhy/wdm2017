from pprint import pprint

import os

import psycopg2
import psycopg2.extensions

from couchdb import Server, Document

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

couch_server = Server()
couch_dbname = 'movies2'
cdb = couch_server['movies2']
print('couch db loaded database: {}'.format(couch_dbname))


def load_movies():
    def process_query(query):
        rows = execute(query)

        movies = []
        for row in rows:
            movies.append({
                'type': 'movie',
                'movie_id': row[0],
                'title': row[1],
                'year': row[2],
                'keywords': row[3],
                'genres': row[4],
                'series': row[5]
            })

        cdb.update(movies)

    movie_id_query = '''SELECT m.idmovies FROM movies AS m'''

    movie_ids = [t[0] for t in execute(movie_id_query)]
    movie_ids.sort()

    num_movies = len(movie_ids)
    stepsize = 50000
    steps = num_movies / stepsize

    basequery = '''SELECT m.idmovies, m.title, m.year, array_agg(DISTINCT k.keyword), array_agg(DISTINCT g.genre), array_agg(DISTINCT s.name)
                FROM movies AS m
                JOIN series AS s ON m.idmovies=s.idmovies
                JOIN movies_keywords AS mk ON m.idmovies=mk.idmovies
                JOIN keywords AS k ON mk.idkeywords=k.idkeywords
                JOIN movies_genres AS mg ON m.idmovies=mg.idmovies
                JOIN genres AS g ON mg.idgenres=g.idgenres
                '''

    print('Loading movies')
    for i in range(0, steps):
        query = basequery + ' WHERE m.idmovies BETWEEN ' + str(movie_ids[i * stepsize]) + ' AND ' + str(
            movie_ids[(i + 1) * stepsize - 1]) + '\n GROUP BY m.idmovies'
        process_query(query)
        print('Completed step {} of {}'.format(i, steps))
    query = basequery + ' WHERE m.idmovies BETWEEN ' + str(movie_ids[steps * stepsize]) + ' AND ' + str(
        movie_ids[-1]) + '\n GROUP BY m.idmovies'
    process_query(query)
    print('Completed step {} of {}'.format(steps, steps))


def load_actors():
    def process_query(query):
        rows = execute(query)

        actors = []
        for row in rows:
            actor = {
                'type': 'actor',
                'idactors': row[0],
                'fname': row[1],
                'lname': row[2],
                'gender': row[3],
                'acted_in': []
            }
            for i, idmovies in enumerate(row[4]):
                actor['acted_in'].append({
                    'idmovies': idmovies,
                    'character': row[5][i]
                })

            actors.append(actor)

        cdb.update(actors)

    idactors_query = '''SELECT a.idactors FROM actors AS a'''

    idactors = [t[0] for t in execute(idactors_query)]
    idactors.sort()

    num_actors = len(idactors)
    stepsize = 50000
    steps = num_actors / stepsize

    basequery = '''SELECT a.idactors, a.fname, a.lname, a.gender, array_agg(ai.idmovies), array_agg(ai.character)
            FROM actors AS a
            JOIN acted_in AS ai ON a.idactors=ai.idactors
            '''

    print('Loading actors')
    for i in range(0, steps):
        query = basequery + ' WHERE a.idactors BETWEEN ' + str(idactors[i * stepsize]) + ' AND ' + str(
            idactors[(i + 1) * stepsize - 1]) + '\n GROUP BY a.idactors'
        process_query(query)
        print('Completed step {} of {}'.format(i, steps))
    query = basequery + ' WHERE a.idactors BETWEEN ' + str(idactors[steps * stepsize]) + ' AND ' + str(
        idactors[-1]) + '\n GROUP BY a.idactors'
    process_query(query)
    print('Completed step {} of {}'.format(steps, steps))


if __name__ == '__main__':
    load_movies()
    load_actors()
