import json

import os

import psycopg2
import psycopg2.extras
import redis
import datetime
import couchdb
from couchdb import Server

database_options = ['postgres', 'redis', 'couchdb']
selected_database_option = database_options[2]

conn = None
try:
    dbname = os.getenv('DBNAME', 'postgres')
    user = os.getenv('DBUSER', 'postgres')
    host = os.getenv('DBHOST', 'localhost')
    password = os.getenv('DBPASSWORD', 'Koekje123')
    conn = psycopg2.connect(dbname=dbname, user=user, host=host, password=password)
    print('connected to postgresql database: {}'.format(dbname))
except:
    print("I am unable to connect to the database, exiting")
    exit(-1)

redisdb=0
r = redis.StrictRedis(host='localhost', port=6379, db=redisdb, decode_responses=True)
print('connected to redis database: {}'.format(redisdb))

couch_server = Server()
couch_dbname = 'movies2'
cdb = couch_server['movies2']
print('connected to couchdb database: {}'.format(couch_dbname))

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
            result = []
            query = ''
            if id:
                query = 'SELECT m.idmovies, m.title, m.year FROM movies AS m WHERE m.idmovies=' + str(id)
            elif title:
                query = 'SELECT m.idmovies, m.title, m.year FROM movies AS m WHERE m.title LIKE \'%' + title + '%\''
            rows = execute(query)

            for row in rows:
                result.append({
                    'idmovies': row[0],
                    'title': row[1],
                    'year': row[2]
                })
            resp.body = json.dumps(result)
        elif selected_database_option == 'redis':
            result = []
            if id:
                ids = [id]
            elif title:
                ids = r.smembers('MOVIESBYTITLE:' + str(title))

            for id in ids:
                idresult = r.hgetall('FMOVIE:' + str(id))
                idresult['idmovies'] = id
                result.append(idresult)

            resp.body = json.dumps(result)
        elif selected_database_option == 'couchdb':
            if id:
                viewresult = cdb.view('movieServiceDesign/SC1_by_id', key=id)
                result = None
                actors = []
                for row in viewresult:
                    value = row['value']
                    if 'idmovies' in value:
                        if result is None:
                            result = value
                        else:
                            raise ValueError("returned multiple movies for this query by id, should not be possible")
                    else:
                        actors.append(value)
                result['actors'] = actors

                resp.body = json.dumps([result])


class SC2ActorResource:
    def on_post(self, req, resp):

        req_as_json = req_to_json(req)
        id = get_param(req_as_json, 'id')
        fname = get_param(req_as_json, 'fname')
        lname = get_param(req_as_json, 'lname')

        if selected_database_option == 'postgres':
            query = None
            basequery = '''SELECT a.idactors, a.fname, a.lname, a.gender, m.idmovies, m.title, m.year 
                        FROM actors AS a 
                        JOIN acted_in AS ai ON a.idactors=ai.idactors
                        JOIN movies AS m ON ai.idmovies=m.idmovies
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

            # Construct a dictionary of actors, in order to return correct results per actor, even if the query
            # results in multiple actors.
            actors = {}
            for row in rows:
                idactors = row[0]
                idmovies = row[4]
                if idactors not in actors:
                    actors[idactors] = {
                        'idactors': idactors,
                        'fname': row[1],
                        'lname': row[2],
                        'gender': row[3],
                        # Use a dict of movies, since an actor can have multiple roles in the same movie, which we only
                        # want to appear as one movie in the result
                        'movies': {
                            idmovies: {
                                'idmovies': idmovies,
                                'title': row[5],
                                'year': row[6]
                            }
                        }
                    }
                else:
                    movies = actors[idactors]['movies']
                    if idmovies not in movies:
                        actors[idactors]['movies'][idmovies] = {
                            'idmovies': idmovies,
                            'title': row[5],
                            'year': row[6]
                        }

            # Return actors as list, as specified
            actors = list(actors.values())

            for actor in actors:
                # Return movies as list ordered by year, as specified
                actor['movies'] = list(actor['movies'].values())
                actor['movies'].sort(key=lambda movie: movie['year'])

            resp.body = json.dumps(actors)
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
                idresult = r.hgetall('ACTOR:' + str(id))
                idresult['idactors'] = id
                result.append(idresult)

            resp.body = json.dumps(result)
        elif selected_database_option == 'couchdb':
            if id:
                viewresult = cdb.view('movieServiceDesign/SC2_by_id', key=id)
                actor = None
                for row in viewresult:
                    if 'idactors' in row['value']:
                        if actor is None:
                            actor = row['value']
                        else:
                            raise ValueError("returned multiple actors for this query by id, should not be possible")
                if actor is not None and 'acted_in' in actor:
                    movieids = [acted_in['movieids'] for acted_in in actor['acted_in']]
                    movies_viewresult = cdb.view('movieServiceDesign/SC1_by_id', keys=movieids)
                    actor['acted_in'] = []
                    for row in movies_viewresult:
                        actor['acted_in'].append(row['value'])
                resp.body = json.dumps([actor])


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
                idresult = r.hgetall('ACTOR:' + str(idactor))
                idresult['acted_in_count'] = r.hget('ACTEDINCOUNT', idactor)
                idresult['idactors'] = idactor
                result.append(idresult)

            resp.body = json.dumps(result)
        elif selected_database_option == 'couchdb':
            if id:
                viewresult = cdb.view('movieServiceDesign/SC3_by_id', key=id)
                results = []
                for result in viewresult:
                    results.append(result['value'])
                resp.body = json.dumps(results)


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
            query += 'AND m.year >= ' + str(fromYear)
            if tillYear:
                query += 'AND m.year < ' + str(tillYear)

            rows = execute(query)
            resp.body = json.dumps(rows)
        if selected_database_option == 'redis':
            result = {}
            ids = r.smembers('GENRES')
            keys = []

            if not tillYear:
                tillYear = datetime.datetime.now().year

            for id in ids:
                for year in range(fromYear, tillYear):
                    keys.append('MOVIESBYGENREBYYEAR:' + str(id) + ':' + str(year))

            idmovies = r.sunion(keys)

            pipe = r.pipeline()
            for idmovie in idmovies:
                pipe.hgetall('MOVIE:' + str(idmovie))
            result = pipe.execute()

            resp.body = json.dumps(result)
        elif selected_database_option == 'couchdb':
            if not tillYear:
                viewresult = cdb.view('movieServiceDesign/SC4_by_id', key=[genre, fromYear])
            else:
                viewresult = cdb.view('movieServiceDesign/SC4_by_id', startkey=[genre, fromYear], endkey=[genre,tillYear])
            results = []
            for result in viewresult:
                results.append(result['value'])
            resp.body = json.dumps(results)


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

            query += 'AND m.year >= ' + str(fromYear)
            if tillYear:
                query += 'AND m.year < ' + str(tillYear)

            query += ' GROUP BY g.idgenres'

            rows = execute(query)
            for row in rows:
                result.append({
                    'movie_count': row[2],
                    'genre': row[1]
                })

            resp.body = json.dumps(result)
        if selected_database_option == 'redis':
            ids = r.smembers('GENRES')
            result = []

            if not tillYear:
                tillYear = datetime.datetime.now().year

            for id in ids:
                genre_name = r.hget('GENRE:' + id, 'genre')
                idresult = dict({
                    'movie_count': 0,
                    'genre': genre_name
                })

                # todo consider replacing with a sunion construct
                for year in range(fromYear, tillYear):
                    idresult['movie_count'] += r.scard('MOVIESBYGENREBYYEAR:' + str(id) + ':' + str(year))
                result.append(idresult)

            resp.body = json.dumps(result)
        elif selected_database_option == 'couchdb':
            if not tillYear:
                viewresult = cdb.view('movieServiceDesign/SC5_by_id', key=[fromYear, {}])
            else:
                viewresult = cdb.view('movieServiceDesign/SC5_by_id', startkey=[fromYear, {}], endkey=[tillYear, "{}"])
            results = []
            for result in viewresult:
                results.append(result['value'])
            resp.body = json.dumps(results)
