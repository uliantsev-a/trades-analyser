from tests.test_config import BaseTestCase
from project.models import PriceHistory
from project.api.serializers import PricesTickSchema


class TestPricesHistory(BaseTestCase):
    fixtures = ['test_data.json']

    def test_diff(self):
        delta_list_close = PriceHistory.get_delta('cvx', 'close', 11)
        delta_list_open = PriceHistory.get_delta('cvx', 'open', 11)

        # 03/01/2018 - 09/01/2018 ~ diff 11 from fixtures
        self.assertEquals(len(delta_list_close), 16)

        # 31/12/2017 - 08/01/2018 ~ diff 11 from fixtures
        self.assertEquals(len(delta_list_open), 9)

    def test_analytics(self):
        data_analytics = PriceHistory.get_analytics(
            'cvx',
            '2017-12-31',
            '2018-01-08'
        )
        prices_schema = PricesTickSchema(
            many=True,
            context={'ticker_name': 'cvx'}
        )
        data = prices_schema.dump(data_analytics)

        self.assertEquals(data.data[0].get('close'), 13)
