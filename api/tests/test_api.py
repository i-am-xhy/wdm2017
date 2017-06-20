import unittest
from unittest import TestCase
import requests
import json

# TODO this should be imported form settings
WAITRESS_PORT = 5000
base_api_url = 'http://localhost:'+str(WAITRESS_PORT)


# todo yes this should be in a subclass of testcase
def has_object_with_key_and_value(self, array, key, value, msg = None):
    try:
        found = False
        for obj in array:
            if not found:
                found = obj[key] == value

        if not msg:
            'Did not find an object with correct key/value pair in results expected for key: '+str(key)+' expected: '+str(value)

        self.assertTrue(found, msg)
    except KeyError:
        self.assertTrue(False, 'Did not find key ' + str(key) + ' in object, original message: ' + msg)



class ActorsTest(TestCase):
    def test_idactors(self):
        r = requests.post(base_api_url + '/actors', json = {
            'id': 13
        })

        has_object_with_key_and_value(self, r.json(), 'fname', 'Ulia')
        has_object_with_key_and_value(self, r.json(), 'lname', 'Hamilton')

    def test_actor_names(self):
        r = requests.post(base_api_url + '/actors', json = {
            'fname': 'Ulia',
            'lname': 'Hamilton'
        })

        has_object_with_key_and_value(self, r.json(), 'idactors', 13)

        r = requests.post(base_api_url + '/actors', json={
            'lname': 'Hamilton'
        })

        has_object_with_key_and_value(self, r.json(), 'idactors', 13)

        r = requests.post(base_api_url + '/actors', json={
            'fname': 'Ulia',
        })

        has_object_with_key_and_value(self, r.json(), 'idactors', 13)


class ShortActorsTest(TestCase):
    def test_idactors(self):
        r = requests.post(base_api_url + '/shortActors', json = {
            'id': 13
        })

        has_object_with_key_and_value(self, r.json(), 'fname', 'Ulia')
        has_object_with_key_and_value(self, r.json(), 'lname', 'Hamilton')
        has_object_with_key_and_value(self, r.json(), 'idactors', 13)
        has_object_with_key_and_value(self, r.json(), 'acted_in_count', 17)

    def test_actor_names(self):
        r = requests.post(base_api_url + '/shortActors', json = {
            'fname': 'Ulia',
            'lname': 'Hamilton'
        })

        has_object_with_key_and_value(self, r.json(), 'idactors', 13)

        # lname hamilton gives a different hamilton, thus the different id to test for
        r = requests.post(base_api_url + '/shortActors', json={
            'lname': 'Hamilton'
        })

        has_object_with_key_and_value(self, r.json(), 'idactors', 2144568)

        # fname Ulia gives a different hamilton, thus the different id to test for
        r = requests.post(base_api_url + '/shortActors', json={
            'fname': 'Ulia',
        })

        has_object_with_key_and_value(self, r.json(), 'idactors', 1082594)


class MoviesTest(TestCase):
    def test_idmovies(self):
        r = requests.post(base_api_url + '/movies', json = {
            'id': 13
        })

        has_object_with_key_and_value(self, r.json(), 'title', 'Mmesis')

    def test_keywords(self):
        r = requests.post(base_api_url + '/movies', json={
            'id': 14
        })

        has_object_with_key_and_value(self, r.json(), 'title', 'Supernatural')
        self.assertEqual(len(r.json()[0]['keywords']), 139)

    def test_keywords(self):
        r = requests.post(base_api_url + '/movies', json={
            'id': 14
        })
        has_object_with_key_and_value(self, r.json(), 'title', 'Supernatural')
        self.assertEqual(len(r.json()[0]['genres']), 5)

    def test_title(self):
        r = requests.post(base_api_url + '/movies', json = {
            'title': 'Night of the Demons'
        })

        has_object_with_key_and_value(self, r.json(), 'title', 'Night of the Demons')
        has_object_with_key_and_value(self, r.json(), 'year', 1988)



    def test_partial_title(self):
        r = requests.post(base_api_url + '/movies', json = {
            'title': 'Star Wars'
        })
        has_object_with_key_and_value(self, r.json(), 'title', 'Star Wars')
        has_object_with_key_and_value(self, r.json(), 'year', 1991)


class GenreTest(TestCase):
    def test_genre(self):
        r = requests.post(base_api_url + '/genres', json = {
            'genre': 'Horror',
            'fromYear': 2012
        })

        self.assertEqual(721, len(r.json()))

        r = requests.post(base_api_url + '/genres', json={
            'genre': 'Horror',
            'fromYear': 2012,
            'tillYear': 2014
        })

        self.assertEqual(285, len(r.json()))


class GenreStatisticsTest(TestCase):
    def test_genre_statistics(self):
        r = requests.post(base_api_url + '/genreStatistics', json = {
            'fromYear': 2012
        })

        self.assertEqual(33, len(r.json()))
        has_object_with_key_and_value(self, r.json(), 'genre', 'History')
        has_object_with_key_and_value(self, r.json(), 'movie_count', 614)

        r = requests.post(base_api_url + '/genreStatistics', json={
            'fromYear': 2012,
            'tillYear': 2014
        })

        self.assertEqual(33, len(r.json()))
        has_object_with_key_and_value(self, r.json(), 'genre', 'Musical')
        has_object_with_key_and_value(self, r.json(), 'movie_count', 73)

if __name__ == '__main__':
    unittest.main()