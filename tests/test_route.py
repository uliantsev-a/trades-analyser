import json
from tests.test_config import BaseTestCase


class TestRoutes(BaseTestCase):
    fixtures = ['test_data.json']

    def test_ticks(self):
        self.assertEquals(True, True)
        with self.app.test_client() as client:
            resp_client = client.get('/api/')
            data = json.loads(resp_client.data)
            self.assertEquals(len(data), 2)
            self.assertEquals(
                [item.get('name') for item in data if item.get('name')],
                ['cvx', 'aapl']
            )
