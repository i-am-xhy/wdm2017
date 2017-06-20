import json

import os

import psycopg2
import psycopg2.extras
import redis
import datetime

database_options = ['postgres', 'redis', 'couchdb']
selected_database_option = database_options[0]

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

r = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)


def execute(command):
    cur = conn.cursor()
    cur.execute(command)
    return cur.fetchall()


def add_api_routes(app):
    app.add_route('/test', testResource())
    app.add_route('/movies', SC1MovieResource())
    app.add_route('/actors', SC2ActorResource())
    app.add_route('/shortActors', SC3ShortActorResource())
    app.add_route('/genres', SC4GenreResource())
    app.add_route('/genreStatistics', SC5GenreStatisticsResource())

def postgres_get_full_movie(id):
    movies = execute('SELECT m.idmovies, m.title, m.year FROM movies as m WHERE idmovies='+str(id))
    if not movies[0]:
        return None
    movie = {
        'idmovies': movies[0][0],
        'title': movies[0][1],
        'year': movies[0][2],
    }
    series = execute('SELECT s.name FROM series as s where s.idmovies = '+str(movie['idmovies']))
    keywords = execute('SELECT k.keyword FROM movies_keywords as mk JOIN keywords as k ON mk.idkeywords=k.idkeywords where mk.idmovies='+str(movie['idmovies']))
    genres = execute('SELECT g.genre FROM movies_genres as mg JOIN genres as g ON mg.idgenres=g.idgenres where mg.idmovies='+str(movie['idmovies']))
    if len(series) == 1:
        movie['names_of_series'] = series.pop()
    movie['keywords'] = keywords
    movie['genres'] = genres

    return movie


def redis_get_full_movie(id):
    idresult = r.hgetall('FMOVIE:' + str(id))
    idresult['idmovies'] = id
    idresult['year'] = int(idresult['year'])
    idresult['genres'] = list(r.smembers('GENRESBYMOVIE:' + str(id)))
    idresult['keywords'] = list(r.smembers('KEYWORDSBYMOVIE:' + str(id)))
    idactors = r.smembers('ACTEDIN:' + str(id))
    idresult['actors'] = {}
    for idactor in idactors:
        idresult['actors'][idactor] = r.get('ROLEBYMOVIEBYACTOR:' + str(id) + ':' + str(idactor))
    return idresult


def redis_get_actor(id):
    idresult = r.hgetall('ACTOR:' + str(id))
    idresult['idactors'] = int(id)
    return idresult


def get_param(json, key):
    if key in json:
        return json[key]
    return None


def req_to_json(req):
    return json.loads(req.stream.read().decode('utf-8'))


class testResource:
    def on_get(self, req, resp):
        resp.body = json.dumps({"test": "success"})


class SC1MovieResource:
    def on_post(self, req, resp):
        req_as_json = req_to_json(req)
        id = get_param(req_as_json, 'id')
        title = get_param(req_as_json, 'title')

        # todo should be full movie not just movie
        if selected_database_option == 'postgres':
            query = ''
            if id:
                query = 'SELECT m.idmovies FROM movies AS m WHERE m.idmovies=' + str(id)
            elif title:
                query = 'SELECT m.idmovies FROM movies AS m WHERE m.title LIKE \'%' + title + '%\''
            rows = execute(query)

            result = []
            for row in rows:
                result.append(postgres_get_full_movie(row[0]))
                # result.append({
                #     'idmovies': row[0],
                #     'title': row[1],
                #     'year': row[2]
                # })
            resp.body = json.dumps(result)
        elif selected_database_option == 'redis':
            result = []
            if id:
                ids = [id]
            elif title:
                ids = r.smembers('MOVIESBYTITLE:' + str(title))

            for id in ids:
                idresult = redis_get_full_movie(id)

                result.append(idresult)

            resp.body = json.dumps(result)


class SC2ActorResource:
    def on_post(self, req, resp):

        req_as_json = req_to_json(req)
        id = get_param(req_as_json, 'id')
        fname = get_param(req_as_json, 'fname')
        lname = get_param(req_as_json, 'lname')

        if selected_database_option == 'postgres':
            query = None
            basequery = '''SELECT a.idactors, a.fname, a.lname, a.gender 
                        FROM actors AS a 
                        '''
            if id:
                query = basequery + ' WHERE a.idactors = ' + str(id)
            elif fname and lname:
                query = basequery + ' WHERE a.fname = \'' + str(fname) + '\' AND a.lname = \'' + str(lname) + '\''
            elif lname:
                query = basequery + ' WHERE a.lname = \'' + str(lname) + '\''
            elif fname:
                query = basequery + ' WHERE a.fname = \'' + str(fname) + '\''

            rows = execute(query)

            result = []
            for row in rows:
                idresult = {
                    'idactors': row[0],
                    'fname': row[1],
                    'lname': row[2],
                    'gender': row[3]
                }

                acted_in_movies = execute('''
                SELECT m.idmovies, m.title, m.year
                FROM acted_in AS ai JOIN movies AS m ON ai.idmovies = m.idmovies
                WHERE ai.idactors=''' + str(idresult['idactors']))
                idresult['movies'] = {}
                for acted_in_movie in acted_in_movies:
                    idresult['movies'][acted_in_movie[0]] = {
                        'idmovies': acted_in_movie[0],
                        'title': acted_in_movie[1],
                        'year': acted_in_movie[2]
                    }
                result.append(idresult)

            resp.body = json.dumps(result)
        elif selected_database_option == 'redis':
            result = []
            ids = []
            if id:
                ids = [id]
            elif fname and lname:
                ids = r.smembers('ACTORSBYFNAMEANDLNAME:' + str(fname) + str(lname))
            elif fname:
                ids = r.smembers('ACTORSBYFNAME:' + str(fname))
            elif lname:
                ids = r.smembers('ACTORSBYLNAME:' + str(lname))

            for id in ids:
                idresult = redis_get_actor(id)

                idmovies = r.smembers('HASACTORS:' + str(id))
                idresult['movies'] = {}
                for idmovie in idmovies:
                    idresult['movies'][idmovie] = r.hgetall('MOVIE:' + str(idmovie))
                result.append(idresult)

            resp.body = json.dumps(result)


class SC3ShortActorResource:
    def on_post(self, req, resp):
        req_as_json = req_to_json(req)
        id = get_param(req_as_json, 'id')
        fname = get_param(req_as_json, 'fname')
        lname = get_param(req_as_json, 'lname')

        if selected_database_option == 'postgres':
            result = []
            query = None
            basequery = '''SELECT a.idactors, a.fname, a.lname, COUNT(idmovies)
                          FROM actors AS a 
                          JOIN acted_in AS ai ON a.idactors=ai.idactors
                        '''
            if id:
                query = basequery + ' WHERE a.idactors = ' + str(id)
            elif fname and lname:
                query = basequery + ' WHERE a.fname = \'' + str(fname) + '\' AND a.lname = \'' + str(lname) + '\''
            elif lname:
                query = basequery + ' WHERE a.lname = \'' + str(lname) + '\''
            elif fname:
                query = basequery + ' WHERE a.fname = \'' + str(fname) + '\''

            query += 'group BY a.idactors, a.fname, a.mname, a.lname'

            rows = execute(query)

            for row in rows:
                result.append({
                    'idactors': row[0],
                    'fname': row[1],
                    'lname': row[2],
                    'acted_in_count': row[3]
                })

            resp.body = json.dumps(result)
        if selected_database_option == 'redis':
            result = []
            if id:
                ids = [id]
            elif fname and lname:
                ids = r.smembers('ACTORSBYFNAMEANDLNAME:' + str(fname) + str(lname))
            elif fname:
                ids = r.smembers('ACTORSBYFNAME:' + str(fname))
            elif lname:
                ids = r.smembers('ACTORSBYLNAME:' + str(lname))

            for idactor in ids:
                idresult = redis_get_actor(idactor)
                idresult['acted_in_count'] = int(r.hget('ACTEDINCOUNT', idactor))
                result.append(idresult)

            resp.body = json.dumps(result)


class SC4GenreResource:
    def on_post(self, req, resp):
        req_as_json = req_to_json(req)
        genre = get_param(req_as_json, 'genre')
        fromYear = get_param(req_as_json, 'fromYear')
        tillYear = get_param(req_as_json, 'tillYear')

        if selected_database_option == 'postgres':
            query = '''SELECT m.idmovies, m.title, m.year
                    FROM genres AS g
                    JOIN movies_genres AS mg ON g.idgenres=mg.idgenres
                    JOIN movies AS m ON m.idmovies = mg.idmovies
            '''

            query += 'WHERE g.genre = \'' + genre + '\''

            if tillYear:
                query += 'AND m.year >= ' + str(fromYear)
                query += 'AND m.year < ' + str(tillYear)
            else:
                query += 'AND m.year = ' + str(fromYear)

            rows = execute(query)
            resp.body = json.dumps(rows)
        if selected_database_option == 'redis':
            result = {}
            idgenres = r.get('GENRESBYNAME:'+genre)

            if not tillYear:
                tillYear = fromYear+1

            keys = []
            for idgenre in idgenres:
                for year in range(fromYear, tillYear):
                    keys.append('MOVIESBYGENREBYYEAR:' + str(idgenre) + ':' + str(year))

            idmovies = r.sunion(keys)

            pipe = r.pipeline()
            for idmovie in idmovies:
                pipe.hgetall('MOVIE:' + str(idmovie))
            result = pipe.execute()

            resp.body = json.dumps(result)


class SC5GenreStatisticsResource:
    def on_post(self, req, resp):
        req_as_json = req_to_json(req)
        fromYear = get_param(req_as_json, 'fromYear')
        tillYear = get_param(req_as_json, 'tillYear')

        if selected_database_option == 'postgres':
            result = []
            query = '''SELECT g.idgenres, g.genre, COUNT(m.idmovies)
                    FROM genres AS g
                    LEFT JOIN movies_genres AS mg ON g.idgenres=mg.idgenres
                    LEFT JOIN movies AS m ON m.idmovies = mg.idmovies
            '''

            if tillYear:
                query += 'AND m.year >= ' + str(fromYear)
                query += 'AND m.year < ' + str(tillYear)
            else:
                query += 'AND m.year = ' + str(fromYear)

            query += ' GROUP BY g.idgenres'

            rows = execute(query)
            for row in rows:
                result.append({
                    'movie_count': row[2],
                    'genre': row[1]
                })

            resp.body = json.dumps(result)
        if selected_database_option == 'redis':
            idgenres = r.smembers('GENRES')
            result = []

            if not tillYear:
                tillYear = fromYear+1

            for idgenre in idgenres:
                genre_name = r.hget('GENRE:' + idgenre, 'genre')

                idresult = dict({
                    'movie_count': 0,
                    'genre': genre_name
                })

                for year in range(fromYear, tillYear):
                    idresult['movie_count'] += r.scard('MOVIESBYGENREBYYEAR:' + str(idgenre) + ':' + str(year))
                result.append(idresult)

            resp.body = json.dumps(result)
