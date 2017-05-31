
import json
from unittest import TestCase
import requests

# TODO this should be imported form settings
WAITRESS_PORT = 5000
base_api_url = 'http://localhost:'+str(WAITRESS_PORT)


class APIIntegrationTest(TestCase):
    def test_api(self):
        tests = [
            self.actorsTest,
        ]

        for test_func in tests:
            with self.subTest(name=test_func.__name__):
                test_func()

    def actorsTest(self):
        r = requests.post(base_api_url + '/actors', json={
            'from': 50,
            'till': 100
        })
        print('result: '+r.text)
        r_as_json = json.loads(r.text)
        self.assertEqual(50, len(r_as_json))