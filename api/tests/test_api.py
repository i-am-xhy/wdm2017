import unittest
from unittest import TestCase
import requests
import json

# TODO this should be imported form settings
WAITRESS_PORT = 5000
base_api_url = 'http://localhost:'+str(WAITRESS_PORT)


# todo yes this should be in a subclass of testcase
def has_object_with_key_and_value(self, array, key, value, msg = 'Did not find an object with correct key/value pair in results'):
    found = False
    for obj in array:
        if not found:
            found = obj[key] == value
    self.assertTrue(found, msg)


class ActorsTest(TestCase):
    def test_idactors(self):
        r = requests.post(base_api_url + '/actors', json = {
            'id': 13
        })

        # r_as_json = r.json()[0]

        has_object_with_key_and_value(self, r.json(), 'fname', 'Ulia')
        has_object_with_key_and_value(self, r.json(), 'lname', 'Hamilton')
        # self.assertEqual('Ulia', r_as_json['fname'])
        # self.assertEqual('Hamilton', r_as_json['lname'])

    def test_actor_names(self):
        r = requests.post(base_api_url + '/actors', json = {
            'fname': 'Ulia',
            'lname': 'Hamilton'
        })

        print(r.json())

        has_object_with_key_and_value(self, r.json(), 'idactors', 13)
        #self.assertEqual(13, r.json()[0]['idactors'])

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

        print('idactors: '+ str(r.json()))

        has_object_with_key_and_value(self, r.json(), 'fname', 'Ulia')
        has_object_with_key_and_value(self, r.json(), 'lname', 'Hamilton')
        has_object_with_key_and_value(self, r.json(), 'idactors', 13)
        has_object_with_key_and_value(self, r.json(), 'acted_in_count', 17)

    def test_actor_names(self):
        r = requests.post(base_api_url + '/shortActors', json = {
            'fname': 'Ulia',
            'lname': 'Hamilton'
        })

        #print('shortactors: ' + str(r.json()))

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
        #print('movie id test')
        r = requests.post(base_api_url + '/movies', json = {
            'id': 13
        })

        #r_as_json = r.json()
        #print('moviejson' + json.dumps(r_as_json))
        #self.assertEqual('Mmesis', r_as_json['title'])
        has_object_with_key_and_value(self, r.json(), 'title', 'Mmesis')

    def test_title(self):
        r = requests.post(base_api_url + '/movies', json = {
            'title': 'Night of the Demons'
        })
        #r_as_json = r.json()
        #print(r_as_json)
        has_object_with_key_and_value(self, r.json(), 'title', 'Night of the Demons')
        has_object_with_key_and_value(self, r.json(), 'year', 1988)
        #self.assertEqual('Night of the Demons', r_as_json['title'])
        #self.assertEqual('1988', r_as_json['year'])

    def test_partial_title(self):
        r = requests.post(base_api_url + '/movies', json = {
            'title': 'Star Wars'
        })
        r_as_json = r.json()
        #print(r_as_json)
        has_object_with_key_and_value(self, r.json(), 'title', 'Star Wars')
        has_object_with_key_and_value(self, r.json(), 'year', 1991)


class GenreTest(TestCase):
    def test_genre(self):
        r = requests.post(base_api_url + '/genres', json = {
            'genre': 'Horror',
            'fromYear': 2012
        })

        r_as_json = r.json()
        self.assertEqual(721, len(r_as_json))

        r = requests.post(base_api_url + '/genres', json={
            'genre': 'Horror',
            'fromYear': 2012,
            'tillYear': 2014
        })

        r_as_json = r.json()
        self.assertEqual(285, len(r_as_json))


class GenreStatisticsTest(TestCase):
    def test_genre_statistics(self):
        r = requests.post(base_api_url + '/genreStatistics', json = {
            'fromYear': 2012
        })

        r_as_json = r.json()

        #print(r_as_json)
        self.assertEqual(33, len(r_as_json))
        has_object_with_key_and_value(self, r.json(), 'genre', 'History')
        has_object_with_key_and_value(self, r.json(), 'movie_count', 614)
        # has_object_with_key_and_value(self, r.json(), 'History', '591')
        # self.assertEqual('History', r_as_json[0][1])
        # self.assertEqual(591, r_as_json[0][2])

        r = requests.post(base_api_url + '/genreStatistics', json={
            'fromYear': 2012,
            'tillYear': 2014
        })

        r_as_json = r.json()
        # print(r_as_json)
        self.assertEqual(33, len(r_as_json))
        has_object_with_key_and_value(self, r.json(), 'genre', 'Musical')
        has_object_with_key_and_value(self, r.json(), 'movie_count', 73)
        # self.assertEqual('Musical', r_as_json[1][1])
        # self.assertEqual(106, r_as_json[1][2])

if __name__ == '__main__':
    unittest.main()