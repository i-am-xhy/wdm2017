import redis
import unittest
from unittest import TestCase

# general redis tests, to see if it is storing proper data

r = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)


class ActorTest(TestCase):
    def test_checkDataExists(self):
        result = r.hgetall('ACTOR:1')
        print(result)
        self.assertEqual('\"\"Steff\"\"', result['lname'])
        self.assertEqual('Stefanie Oxmann Mcgaha', result['fname'])
        self.assertEqual('None', result['gender'])
    def test_actor_by_fname(self):
        id = r.hget('ACTORBYFNAME', 'Remi')
        result = r.hgetall('ACTOR:'+str(id))
        self.assertEqual('Aquino', result['lname'])
        self.assertEqual('Remi', result['fname'])
        self.assertEqual('None', result['gender'])

class MovieTest(TestCase):
    def test_movie_data_exists(self):
        result = r.hgetall('MOVIE:1')
        #print(result)
        self.assertEqual('Night of the Demons', result['title'])
        self.assertEqual('2009', result['year'])

    def test_movie_by_title(self):
        id = r.hget('MOVIEBYTITLE', 'Rebellious Flower')
        #print(r.hgetall('MOVIEBYTITLE'))
        #print('id for title '+ str(id))
        result = r.hgetall('MOVIE:'+str(id))
        #print('movie by title result')
        #print(result)
        self.assertEqual('Rebellious Flower', result['title'])
        self.assertEqual('2016', result['year'])

    def test_full_movie_data_exists(self):
        result = r.hgetall('FMOVIE:1')
        #print(result)
        self.assertEqual('Night of the Demons', result['title'])
        self.assertEqual('2009', result['year'])
        self.assertEqual('still need to add other values for fmovie', None)




if __name__ == '__main__':
    unittest.main()
