import json
import psycopg2

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


def add_api_routes(app):
    app.add_route('/test', testResource())
    app.add_route('/movies', movieResource())
    app.add_route('/actors', actorResource())


class testResource:
    def on_get(self, req, resp):
        resp.body = json.dumps({"test": "success"})


class movieResource:
    def on_get(self, req, resp):
        query = 'SELECT * FROM movies LIMIT 1'
        rows = execute(query)
        result = rows[0]
        resp.body = json.dumps(result)


class actorResource:
    def on_get(self, req, resp):
        # yeah i am adding some extra fields, mostly to have some joining in there.
        query = '''SELECT * FROM actors as a
                  join acted_in as ai on  a.idactors=ai.idactors 
                  join movies as m on m.idmovies=ai.idmovies 
                  LIMIT 1'''
        rows = execute(query)
        result = rows[0]
        resp.body = json.dumps(result)

    def on_post(self, req, resp):
        req_as_json = json.loads(req.stream.read().decode('utf-8'))
        start = req_as_json['from']
        end = req_as_json['till']

        amount = max(end-start-1, start)
        offset = start

        query = 'SELECT * FROM actors LIMIT ' + str(amount) + ' OFFSET ' + str(offset)
        rows = execute(query)
        resp.body = json.dumps(rows)
#
#         resp.body = json.dumps(answers_list)
