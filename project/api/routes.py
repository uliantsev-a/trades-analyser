# project/api/routes.py

from flask import Blueprint, request
from project.models import Ticker, PriceHistory, Insider, Trade
from project.api.serializers import (
    TickSchema,
    PricesTickSchema,
    InsiderSchema,
    InsiderTradeSchema,
    AnalyticsPriceSchema,
    DeltaPriceSchema,
    DeltaListSchema,
)
from project.utils import get_object_or_404

mod_api = Blueprint('api', __name__,)


@mod_api.route('/')
def get_tickers():
    ticks = Ticker.query.all()
    tick_schema = TickSchema(many=True)
    return tick_schema.jsonify(ticks)


@mod_api.route('/<ticker_name>/insider/')
def get_insiders(ticker_name):
    insiders = Insider.query.join(Insider.trades, aliased=True).filter(Ticker.name == ticker_name)
    insider_schema = InsiderSchema(many=True, context={'ticker_name': ticker_name})
    return insider_schema.jsonify(insiders)


@mod_api.route('/<ticker_name>/insider/<insider_name>/')
def get_insider_trades(ticker_name, insider_name):
    trade_list = Trade.query.join(Trade.insider).join(Trade.ticker).filter(
        Ticker.name == ticker_name,
        Insider.name == insider_name
    )

    trade_schema = InsiderTradeSchema(many=True)
    return trade_schema.jsonify(trade_list)


@mod_api.route('/<ticker_name>/analytics/')
def get_analytics_prices(ticker_name):
    params = request.args.to_dict()
    params.update({'ticker_name': ticker_name})
    analytics_scheme = AnalyticsPriceSchema()
    result = analytics_scheme.load(params)

    prices_list = PriceHistory.get_analytics(**result.data)
    prices_schema = PricesTickSchema(many=True, context={'ticker_name': ticker_name})
    return prices_schema.jsonify(prices_list)


@mod_api.route('/<ticker_name>/delta/')
def get_delta_prices(ticker_name):
    get_object_or_404(Ticker, Ticker.name == ticker_name)
    params = request.args.to_dict()
    params.update({'ticker_name': ticker_name})
    delta_scheme = DeltaPriceSchema()
    scheme_args = delta_scheme.load(params)

    prices_list = PriceHistory.get_delta(**scheme_args.data)
    prices_schema = DeltaListSchema(many=True)
    return prices_schema.jsonify(prices_list)


@mod_api.route('/<ticker_name>/')
def get_tick_prices(ticker_name):
    prices_list = PriceHistory.query.filter(PriceHistory.ticker.has(name=ticker_name))
    prices_schema = PricesTickSchema(many=True)
    return prices_schema.jsonify(prices_list)
