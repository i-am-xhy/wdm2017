import redis
import psycopg2
import json
from joblib import Parallel, delayed
import multiprocessing

r = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)


def set_data_for_actor(actor, connection=r):
    connection.hset('ACTOR:' + str(actor[0]), 'fname', actor[1])
    connection.hset('ACTOR:' + str(actor[0]), 'lname', actor[2])
    connection.hset('ACTOR:' + str(actor[0]), 'gender', actor[3])
    connection.hset('ACTEDINCOUNT', actor[0], actor[4])

    connection.sadd('ACTORSBYFNAMEANDLNAME:' + str(actor[1]) + str(actor[2]), actor[0])
    connection.sadd('ACTORSBYFNAME:' + str(actor[1]), actor[0])
    connection.sadd('ACTORSBYLNAME:' + str(actor[2]), actor[0])


def set_data_for_movie(movie):
    r.hset('MOVIE:' + str(movie[0]), 'title', movie[1])
    r.hset('MOVIE:' + str(movie[0]), 'year', movie[2])
    r.hset('FMOVIE:' + str(movie[0]), 'title', movie[1])
    r.hset('FMOVIE:' + str(movie[0]), 'year', movie[2])
    r.sadd('MOVIESBYTITLE:' + str(movie[1]), movie[0])


def set_data_for_movies_genre(mg):
    #r.sadd('MOVIESBYGENRE:'+str(mg[2]), mg[1])
    r.sadd('MOVIESBYGENREBYYEAR:'+str(mg[2]) + ':'+str(mg[3]), mg[1])
    r.sadd('GENRESBYMOVIE:' + str(mg[1]), mg[2])


def set_data_for_acted_in(ai):
    r.sadd('ACTEDIN:' + str(ai[1]), ai[3])
    r.sadd('HASACTORS:' + str(ai[3]), ai[1])

    r.set('ROLEBYMOVIEBYACTOR:' + str(ai[1]) + ':' + str(ai[3]), ai[4])
    #r.set('ROLE:' + ai[3], ai[4])



def set_data_for_genre(g):
    r.hset('GENRE:' + str(g[0]), 'genre', g[1])
    r.sadd('GENRES', g[0])
    r.set('GENRESBYNAME:' + str(g[1]), g[0])


def set_data_for_series(s):
    r.hset('FMOVIE:' + str(s[1]), 'name_of_series', s[2])


def set_data_for_movies_keywords(mk):
    r.sadd('KEYWORDSBYMOVIE:' + str(mk[1]), mk[5])

if __name__ == '__main__':
    # for quick adaptability of the script add a limit to see if it works at all.
    # for a correct database transfer make this an empty string
    potentialLimit = ''#'' LIMIT 1000000'

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
    # actors
    # pipe = r.pipeline()
    actors = execute('select a.idactors, a.fname, a.lname, a.gender, count(a.idactors) as moviecount from actors as a join acted_in as ai on a.idactors=ai.idactors group by a.idactors' + potentialLimit)
    Parallel(n_jobs=num_cores, verbose=5)(delayed(set_data_for_actor)(actor) for actor in actors)
    # print(len(actors))
    # i = 0
    # for actor in actors:
    #     if i/len(actors) % 0.01 == 0:
    #         print('at ' + str(i) + 'out of ' + str(len(actors)))
    #     set_data_for_actor(actor, pipe)
    # pipe.execute()

    # movies
    movies = execute('SELECT * FROM movies' + potentialLimit)
    Parallel(n_jobs=num_cores, verbose=5)(delayed(set_data_for_movie)(movie) for movie in movies)

    # genres
    genres = execute('SELECT * FROM genres' + potentialLimit)
    Parallel(n_jobs=num_cores, verbose=5)(delayed(set_data_for_genre)(genre) for genre in genres)

    # movies_genres
    movies_genres = execute('select mg.idmovies_genres, mg.idmovies, mg.idgenres, m.year  from movies_genres as mg join movies as m on mg.idmovies=m.idmovies' + potentialLimit)
    Parallel(n_jobs=num_cores, verbose=5)(delayed(set_data_for_movies_genre)(movies_genre) for movies_genre in movies_genres)

    # acted_in
    cur = conn.cursor('acted in nightmare cursor, store on server to prevent out of memory')
    cur.itersize = 100000
    cur.execute('select * from acted_in'+potentialLimit)
    Parallel(n_jobs=num_cores, verbose=5)(delayed(set_data_for_acted_in)(acted_in) for acted_in in cur)

    # serie name
    series = execute('SELECT * FROM series')
    Parallel(n_jobs=num_cores, verbose=5)(delayed(set_data_for_series)(serie) for serie in series)

    # keywords
    movies_keywords = execute('SELECT * FROM movies_keywords AS mk JOIN keywords AS k ON mk.idkeywords=k.idkeywords')
    Parallel(n_jobs=num_cores, verbose=5)(delayed(set_data_for_movies_keywords)(movie_keyword) for movie_keyword in movies_keywords)

