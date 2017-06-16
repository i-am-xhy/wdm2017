import json
import os

import psycopg2

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

        if selected_database_option == 'postgres':
            query = ''
            if id:
                query = 'SELECT m.idmovies, m.title, m.year FROM movies as m where m.idmovies=' + str(id)
            elif title:
                query = 'SELECT m.idmovies, m.title, m.year FROM movies as m where m.title LIKE \'%' + title + '%\''
            rows = execute(query)
            result = rows[0]
            resp.body = json.dumps(result)


class SC2ActorResource:
    def on_post(self, req, resp):

        req_as_json = req_to_json(req)
        id = get_param(req_as_json, 'id')
        fname = get_param(req_as_json, 'fname')
        lname = get_param(req_as_json, 'lname')

        if selected_database_option == 'postgres':
            query = None
            basequery = '''SELECT a.idactors, a.fname, a.lname, a.gender, m.idmovies, m.title, m.year 
                        FROM actors as a 
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
            movies = [
                {
                    'id': row[4],
                    'title': row[5],
                    'year': row[6],
                }
                for row in rows
            ]
            # TODO: try to return unique movies from database in query, to avoid filtering in python
            unique_movies = list({movie['id']: movie for movie in movies}.values())
            if len(rows) > 0:
                result = {
                    'first_name': rows[0][1],
                    'last_name': rows[0][2],
                    'gender': rows[0][3],
                    'movies': unique_movies
                }

            resp.body = json.dumps(result)


class SC3ShortActorResource:
    def on_post(self, req, resp):
        req_as_json = req_to_json(req)
        id = get_param(req_as_json, 'id')
        fname = get_param(req_as_json, 'fname')
        lname = get_param(req_as_json, 'lname')

        if selected_database_option == 'postgres':
            query = None
            basequery = '''SELECT a.idactors, a.fname, a.mname, a.lname, COUNT(idmovies)
                          FROM actors as a 
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

            result = [
                {
                    'first_name': row[1],
                    'middle_name': row[2],
                    'last_name': row[3],
                    'movies_count': row[4]
                }
                for row in rows
            ]

            resp.body = json.dumps(result)


class SC4GenreResource:
    def on_post(self, req, resp):
        req_as_json = req_to_json(req)
        genre = get_param(req_as_json, 'genre')
        fromYear = get_param(req_as_json, 'fromYear')
        tillYear = get_param(req_as_json, 'tillYear')

        if selected_database_option == 'postgres':
            query = '''SELECT a.idactors, a.fname, a.lname, a.gender, m.idmovies, m.title, m.year
                    FROM genres AS g
                    JOIN movies_genres AS mg ON g.idgenres=mg.idgenres
                    JOIN movies AS m ON m.idmovies = mg.idmovies
                    JOIN acted_in AS ai ON m.idmovies = ai.idmovies
                    JOIN actors AS a ON a.idactors = ai.idactors
            '''

            query += 'WHERE g.genre = \'' + genre + '\''
            query += 'AND m.year >= ' + str(fromYear)
            if tillYear:
                query += 'AND m.year <= ' + str(tillYear)

            rows = execute(query)
            resp.body = json.dumps(rows)


class SC5GenreStatisticsResource:
    def on_post(self, req, resp):
        req_as_json = req_to_json(req)
        fromYear = get_param(req_as_json, 'fromYear')
        tillYear = get_param(req_as_json, 'tillYear')

        if selected_database_option == 'postgres':
            query = '''SELECT g.idgenres, g.genre, COUNT(m.idmovies)
                    FROM genres AS g
                    JOIN movies_genres AS mg ON g.idgenres=mg.idgenres
                    JOIN movies AS m ON m.idmovies = mg.idmovies
            '''

            query += 'AND m.year >= ' + str(fromYear)
            if tillYear:
                query += 'AND m.year <= ' + str(tillYear)

            query += ' GROUP BY g.idgenres'

            rows = execute(query)
            resp.body = json.dumps(rows)
