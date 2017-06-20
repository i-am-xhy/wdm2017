from pprint import pprint

import os

import psycopg2
import psycopg2.extensions

from couchdb import Server, Document

from api.code.apiserver import execute

conn = None
try:
    dbname = os.getenv('DBNAME', 'postgres')
    user = os.getenv('DBUSER', 'postgres')
    host = os.getenv('DBHOST', 'localhost')
    password = os.getenv('DBPASSWORD', 'Koekje123')
    conn = psycopg2.connect(dbname=dbname, user=user, host=host, password=password)
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, conn)
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
    stepsize = 20000
    steps = int(num_movies / stepsize)

    # We denormalize all keywords, series and genres by inlining them into the movie object. This allows for efficient
    # queries, but not for efficient updates. Since changes to series, keywords or genres are very rare, this is
    # appropriate for our use case.
    basequery = '''SELECT m.idmovies, m.title, m.year, array_agg(DISTINCT k.keyword), array_agg(DISTINCT g.genre), array_agg(DISTINCT s.name)
                FROM movies AS m
                LEFT JOIN series AS s ON m.idmovies=s.idmovies
                LEFT JOIN movies_keywords AS mk ON m.idmovies=mk.idmovies
                LEFT JOIN keywords AS k ON mk.idkeywords=k.idkeywords
                LEFT JOIN movies_genres AS mg ON m.idmovies=mg.idmovies
                LEFT JOIN genres AS g ON mg.idgenres=g.idgenres
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
            # We use the "List of Keys" approach for Many-to-Many relationships to describe the acted_in relationship
            # between actors and movies, as outlined here:
            # https://wiki.apache.org/couchdb/EntityRelationship#Many_to_Many:_List_of_Keys.
            #
            # This approach is suitable for our application, since it allows for efficient querying without duplicating
            # any data. It also allows for efficient updates, although it is not optimized for concurrent updates
            # to to same actor, since this can cause conflicts. This is also appropriate, since actors will rarely
            # need to be updated by multiple people at the same time.
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
    steps = int(num_actors / stepsize)

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
