import redis
import psycopg2
import json
from joblib import Parallel, delayed
import multiprocessing

r = redis.StrictRedis(host='localhost', port=6379, db=0)

def set_data_for_actor(actor):
    r.hset('ACTOR:' + str(actor[0]), 'fname', actor[2])
    r.hset('ACTOR:' + str(actor[0]), 'lname', actor[1])
    r.hset('ACTOR:' + str(actor[0]), 'gender', actor[4])
    r.hset('ACTORBYFNAMEANDLNAME', str(actor[2]) + str(actor[1]), actor[0])
    r.hset('ACTORBYFNAME', str(actor[2]), actor[0])
    r.hset('ACTORBYLNAME', str(actor[1]), actor[0])


def set_data_for_movie(movie):
    r.hset('MOVIE:' + str(movie[0]), 'title', movie[1])
    r.hset('MOVIE:' + str(movie[0]), 'year', movie[2])
    r.hset('FMOVIE:' + str(movie[0]), 'title', movie[1])
    r.hset('FMOVIE:' + str(movie[0]), 'year', movie[2])
    r.hset('MOVIEBYTITLE', movie[1], movie[0])

if __name__ == '__main__':
    # for quick adaptability of the script add a limit to see if it works at all.
    # for a correct database transfer make this an empty string
    potentialLimit = ''#' LIMIT 10000'

    # todo refactor with apiserver.py to have common source
    conn = None
    try:
        conn = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='Koekje123'")
    except:
        print("I am unable to connect to the database, exitting")
        exit(-1)

    def execute(command):
        cur = conn.cursor()
        cur.execute(command)
        return cur.fetchall()

    num_cores = multiprocessing.cpu_count()
    print('num cores: '+str(num_cores))

    actors = execute('SELECT * FROM actors' + potentialLimit)
    Parallel(n_jobs=num_cores)(delayed(set_data_for_actor)(actor) for actor in actors)
    print('done with actors')

    movies = execute('SELECT * FROM movies' + potentialLimit)
    Parallel(n_jobs=num_cores)(delayed(set_data_for_movie)(movie) for movie in movies)
    print('done with movies')


    #genres = execute('SELECT * FROM genres' + potentialLimit)

    # for genre in genres:
    #     r.hset('')