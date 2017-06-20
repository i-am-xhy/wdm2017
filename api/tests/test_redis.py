import redis
import unittest
from unittest import TestCase

# general redis tests, to see if it is storing proper data

# r = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
#
# def has_object_with_key_and_value(self, array, key, value, msg = 'Did not find an object with correct key/value pair in results'):
#     found = False
#     for obj in array:
#         if not found:
#             found = obj[key] == value
#     self.assertTrue(found, msg)
#
# class ActorTest(TestCase):
#     def test_checkDataExists(self):
#         result = r.hgetall('ACTOR:1')
#         self.assertEqual('\"\"Steff\"\"', result['lname'])
#         self.assertEqual('Stefanie Oxmann Mcgaha', result['fname'])
#         self.assertEqual('None', result['gender'])
#     def test_actor_by_fname(self):
#         ids = r.smembers('ACTORSBYFNAME:Remi')
#         self.assertIn('748468', ids)
#
#         result = r.hgetall('ACTOR:748468')
#
#         self.assertEqual('Mayama', result['lname'])
#         self.assertEqual('Remi', result['fname'])
#         self.assertEqual('None', result['gender'])
#
# class MovieTest(TestCase):
#     def test_movie_data_exists(self):
#         result = r.hgetall('MOVIE:1')
#         #print(result)
#         self.assertEqual('Night of the Demons', result['title'])
#         self.assertEqual('2009', result['year'])
#
#     def test_movie_by_title(self):
#         ids = r.smembers('MOVIESBYTITLE:Rebellious Flower')
#         result = r.hgetall('MOVIE:'+str(ids.pop()))
#         self.assertEqual('Rebellious Flower', result['title'])
#         self.assertEqual('2016', result['year'])
#
#     def test_full_movie_data_exists(self):
#         result = r.hgetall('FMOVIE:1')
#         self.assertEqual('Night of the Demons', result['title'])
#         self.assertEqual('2009', result['year'])




if __name__ == '__main__':
    unittest.main()
