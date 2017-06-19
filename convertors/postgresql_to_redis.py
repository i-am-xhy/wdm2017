import redis
import psycopg2
import json
from joblib import Parallel, delayed
import multiprocessing

r = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

def set_data_for_actor(actor):
    r.hset('ACTOR:' + str(actor[0]), 'fname', actor[1])
    r.hset('ACTOR:' + str(actor[0]), 'lname', actor[2])
    r.hset('ACTOR:' + str(actor[0]), 'gender', actor[3])
    r.hset('ACTEDINCOUNT', actor[0], actor[4])

    r.sadd('ACTORSBYFNAMEANDLNAME:' + str(actor[1]) + str(actor[2]), actor[0])
    r.sadd('ACTORSBYFNAME:' + str(actor[1]), actor[0])
    r.sadd('ACTORSBYLNAME:' + str(actor[2]), actor[0])


def set_data_for_movie(movie):
    r.hset('MOVIE:' + str(movie[0]), 'title', movie[1])
    r.hset('MOVIE:' + str(movie[0]), 'year', movie[2])
    r.hset('FMOVIE:' + str(movie[0]), 'title', movie[1])
    r.hset('FMOVIE:' + str(movie[0]), 'year', movie[2])

    r.sadd('MOVIESBYTITLE:' + str(movie[1]), movie[0])

def set_data_for_movies_genre(mg):
    #r.sadd('MOVIESBYGENRE:'+str(mg[2]), mg[1])
    r.sadd('MOVIESBYGENREBYYEAR:'+str(mg[2]) + ':'+str(mg[3]), mg[1])

def set_data_for_acted_in(ai):
    r.sadd('ACTEDIN:' + str(ai[1]), ai[3])
    r.sadd('HASACTORS:' + str(ai[3]), ai[1])

def set_data_for_genre(g):
    r.hset('GENRE:' + str(genre[0]), 'genre', genre[1])
    r.sadd('GENRES', genre[0])


if __name__ == '__main__':
    # for quick adaptability of the script add a limit to see if it works at all.
    # for a correct database transfer make this an empty string
    potentialLimit = ''#'' LIMIT 10000'

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

    # todo we probably don't want this to happen always
    r.flushdb()

    actors = execute('select a.idactors, a.fname, a.lname, a.gender, count(a.idactors) as moviecount from actors as a join acted_in as ai on a.idactors=ai.idactors group by a.idactors' + potentialLimit)
    Parallel(n_jobs=num_cores, verbose=5)(delayed(set_data_for_actor)(actor) for actor in actors)
    print('done with actors')

    movies = execute('SELECT * FROM movies' + potentialLimit)
    Parallel(n_jobs=num_cores, verbose=5)(delayed(set_data_for_movie)(movie) for movie in movies)
    print('done with movies')


    genres = execute('SELECT * FROM genres' + potentialLimit)

    for genre in genres:
        set_data_for_genre(genre)

    movies_genres = execute('select mg.idmovies_genres, mg.idmovies, mg.idgenres, m.year  from movies_genres as mg join movies as m on mg.idmovies=m.idmovies' + potentialLimit)
    Parallel(n_jobs=num_cores, verbose=5)(delayed(set_data_for_movies_genre)(movies_genre) for movies_genre in movies_genres)

    cur = conn.cursor('acted in nightmare cursor')
    cur.itersize = 100000
    cur.execute('select * from acted_in'+potentialLimit)
    Parallel(n_jobs=num_cores, verbose=5)(delayed(set_data_for_acted_in)(acted_in) for acted_in in cur)

