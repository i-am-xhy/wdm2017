import unittest
from unittest import TestCase
import requests

# TODO this should be imported form settings
WAITRESS_PORT = 5000
base_api_url = 'http://localhost:'+str(WAITRESS_PORT)


class ActorsTest(TestCase):
    def test_idactors(self):
        r = requests.post(base_api_url + '/actors', json = {
            'id': 13
        })

        r_as_json = r.json()[0]

        self.assertEqual('Ulia', r_as_json[1])
        self.assertEqual('Hamilton', r_as_json[2])

    def test_actor_names(self):
        r = requests.post(base_api_url + '/actors', json = {
            'fname': 'Ulia',
            'lname': 'Hamilton'
        })

        self.assertEqual(13, r.json()[0][0])

        r = requests.post(base_api_url + '/actors', json={
            'lname': 'Hamilton'
        })

        self.assertEqual(13, r.json()[0][0])

        r = requests.post(base_api_url + '/actors', json={
            'fname': 'Ulia',
        })

        self.assertEqual(13, r.json()[0][0])


class shortActorsTest(TestCase):
    def test_idactors(self):
        r = requests.post(base_api_url + '/shortActors', json = {
            'id': 13
        })

        r_as_json = r.json()[0]

        self.assertEqual('Ulia', r_as_json[1])
        self.assertEqual('Hamilton', r_as_json[3])
        self.assertEqual(17, r_as_json[4])

    def test_actor_names(self):
        r = requests.post(base_api_url + '/shortActors', json = {
            'fname': 'Ulia',
            'lname': 'Hamilton'
        })

        self.assertEqual(13, r.json()[0][0])

        # lname hamilton gives a different hamilton, thus the different id to test for
        r = requests.post(base_api_url + '/shortActors', json={
            'lname': 'Hamilton'
        })

        self.assertEqual(2144568, r.json()[0][0])

        # fname Ulia gives a different hamilton, thus the different id to test for
        r = requests.post(base_api_url + '/shortActors', json={
            'fname': 'Ulia',
        })

        self.assertEqual(1082594, r.json()[0][0])


class MoviesTest(TestCase):
    def test_idmovies(self):
        print('movie id test')
        r = requests.post(base_api_url + '/movies', json = {
            'id': 13
        })

        r_as_json = r.json()
        self.assertEqual('Mmesis', r_as_json[1])

    def test_title(self):
        r = requests.post(base_api_url + '/movies', json = {
            'title': 'Star Wars'
        })
        r_as_json = r.json()
        self.assertEqual('Star Wars: Battlefront', r_as_json[1])


class GenreTest(TestCase):
    def test_genre(self):
        r = requests.post(base_api_url + '/genres', json = {
            'genre': 'Horror',
            'fromYear': 2012
        })

        r_as_json = r.json()
        self.assertEqual(39100, len(r_as_json))

        r = requests.post(base_api_url + '/genres', json={
            'genre': 'Horror',
            'fromYear': 2012,
            'tillYear': 2014
        })

        r_as_json = r.json()
        self.assertEqual(29336, len(r_as_json))


class GenreStatisticsTest(TestCase):
    def test_genre_statistics(self):
        r = requests.post(base_api_url + '/genreStatistics', json = {
            'fromYear': 2012
        })

        r_as_json = r.json()
        self.assertEqual(27, len(r_as_json))
        self.assertEqual('History', r_as_json[0][1])
        self.assertEqual(614, r_as_json[0][2])

        r = requests.post(base_api_url + '/genreStatistics', json={
            'fromYear': 2012,
            'tillYear': 2014
        })

        r_as_json = r.json()
        self.assertEqual(27, len(r_as_json))
        self.assertEqual('Musical', r_as_json[1][1])
        self.assertEqual(106, r_as_json[1][2])

if __name__ == '__main__':
    unittest.main()