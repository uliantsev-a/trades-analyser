import re
import sys
import enum
import asyncio
import requests
from lxml import html
from datetime import date, datetime

from project import db
from project.models import (
    Trade,
    Insider,
    TransactionType,
    Ticker,
    PriceHistory,
)
from project.utils import get_or_create

MAX_PAGES = 10
STR_DATE = '%m/%d/%Y'
FORMAT_TIME = r'^\d{2}:\d{2}'


@enum.unique
class TypeScrap(enum.Flag):
    """ Available types for scrapping

    """
    PRICE = 'price'
    TRADE = 'trade'
    ALL = 'all'


class TradingScraper:
    """Trading scrapper for site on www.nasdaq.com

    """
    URL_PRICES = 'http://www.nasdaq.com/symbol/{}/historical'
    URL_TRADES = 'https://www.nasdaq.com/symbol/{}/insider-trades?page={}'
    XPATH_TRADES = '//*[@id="content_main"]/div[8]/div[5]/table/tr'
    XPATH_PRICES = '//table/tbody/tr'
    XPATH_LAST_PAGE = 'substring-after(//*[@id="quotes_content_left_lb_LastPage"]/@href, "?page=")'
    PRICES_INTERVAL = '3m'

    def __init__(self, trick_name, scraping_type=None):
        self.trick_name = trick_name.lower()
        self.prices_done = False
        self.paging = 0
        self.pages_count = MAX_PAGES
        self.started_paging = False
        self.last_page = None
        self.finished = False
        if scraping_type in TypeScrap:
            self.scrap_type = scraping_type
        else:
            self.scrap_type = TypeScrap.ALL

    def __await__(self):
        if not self.prices_done and self.scrap_type in (TypeScrap.PRICE, TypeScrap.ALL):
            self.prices_done = True
            return self.scraping_prices().__await__()
        elif self.paging < self.pages_count and self.scrap_type in (TypeScrap.TRADE, TypeScrap.ALL):
            if not self.paging:
                self.started_paging = True
            self.paging += 1
            return self.scraping_trades(self.paging).__await__()
        else:
            self.finished = True
            return asyncio.sleep(0)

    async def scraping_trades(self, page_index):
        """Scraping of trades price from specific index page
        :param page_index: specific paging
        :return: generator with needed values in lines from table
        """

        page = requests.get(self.URL_TRADES.format(self.trick_name, page_index))
        tree = html.fromstring(page.content)
        trades = (
            [
                tr.xpath('substring-after(td[1]/a/@href, "insiders/")'),
                tr.xpath('string(td[1]/a/text())'),
                tr.xpath('string(td[7]/text())'),
            ] + tr.xpath('td[position() > 1][position() != 6]/text()')
            for tr in
            tree.xpath(self.XPATH_TRADES)
        )

        if not self.last_page:
            self.last_page = int(tree.xpath(self.XPATH_LAST_PAGE))
            if self.last_page < self.pages_count:
                self.pages_count = self.last_page
        return trades_agent(trades)

    async def scraping_prices(self):
        """ Scrapping of historical prices for the specific interval
        :return: generator with needed values in lines from table
        """

        payload = '{}|false|{}'.format(self.PRICES_INTERVAL, self.trick_name)
        page = requests.post(self.URL_PRICES.format(self.trick_name), data=payload)
        tree = html.fromstring(page.content)
        prices = (
            list(map(
                lambda td: td.replace('\r\n', '').strip(),
                tr.xpath('td/text()')
            ))
            for tr in
            tree.xpath(self.XPATH_PRICES)
        )
        return prices_agent(prices)


def trades_agent(generator):
    yield from generator


def prices_agent(generator):
    yield from generator


def send_tasks_to_load(tick_name, tasks):
    for task in tasks:
        agent = task.result()
        if agent is None:
            continue

        if agent.__name__ == 'prices_agent':
            prices_loading(tick_name, agent)
        elif agent.__name__ == 'trades_agent':
            trades_loading(tick_name, agent)


def prices_loading(trick_name, prices):
    print('PRICES_LOADING')
    today = date.today()
    ticker = get_or_create(Ticker, name=trick_name)
    order_params = {'date': 0, 'open': 1, 'high': 2, 'low': 3, 'close': 4, 'volume': 5}
    for row in prices:
        if not any(row):
            continue

        params = dict((key, row[index]) for key, index in order_params.items())
        param_date = params.pop('date')
        if re.match(FORMAT_TIME, param_date):
            write_date = today
        else:
            write_date = datetime.strptime(param_date, STR_DATE).date()
        price_record, created = PriceHistory.get_or_create(ticker=ticker, date=write_date, **params)
        if not created:
            price_record.update(**params)
    db.session.commit()


def trades_loading(trick_name, trades):
    print('TRADES_LOADING')
    order_params = {
        'code': 0,
        'insider': 1,
        'last_price': 2,
        # 'relation': 3,
        'last_date': 4,
        'transaction_type': 5,
        # 'owner_type': 6,
        'shares_traded': 7,
        'shares_held': 8
    }
    ticker = get_or_create(Ticker, name=trick_name)
    for row in trades:
        params = dict((key, row[index]) for key, index in order_params.items())
        last_date = datetime.strptime(params.pop('last_date'), STR_DATE).date()
        insider_code = params.pop('code', '').split('-')[-1]
        insider = get_or_create(Insider, name=params.pop('insider'), code=insider_code)
        transaction_type = get_or_create(TransactionType, name=params.pop('transaction_type'))

        trade_record, created = Trade.get_or_create(
            ticker=ticker,
            insider=insider,
            last_date=last_date,
            transaction_type=transaction_type,
            **params
        )

        if not created:
            trade_record.update(**params)
    db.session.commit()


async def main(event_loop, ticks_list, threads_limit=10, types_scrubs=None):
    dl_tasks = set()
    scraper = TradingScraper(ticks_list.pop(0), types_scrubs)
    while not scraper.finished:
        dl_tasks.add(event_loop.create_task(scraper.__await__()))

        if len(dl_tasks) >= threads_limit or scraper.started_paging > 0:
            # Wait for some download to finish before adding a new one
            _done, dl_tasks = await asyncio.wait(
                dl_tasks, return_when=asyncio.FIRST_COMPLETED
            )
            send_tasks_to_load(scraper.trick_name, _done)

        if scraper.finished and len(ticks_list):
            scraper = TradingScraper(ticks_list.pop(0), types_scrubs)

    if len(dl_tasks):
        # Wait for the remaining downloads to finish
        _done, _ = await asyncio.wait(dl_tasks)
        send_tasks_to_load(scraper.trick_name, _done)


def run(file_path, threads_limit, types_scrubs=None):
    tick_file = open(file_path, 'r')
    tick_list = tick_file.read().splitlines()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop, tick_list, threads_limit=threads_limit, types_scrubs=types_scrubs))


if __name__ == '__main__':
    threads = 5
    if len(sys.argv) > 1:
        threads = int(sys.argv[1])
    run('tickers.txt', threads)
