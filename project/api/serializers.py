import re
from project import ma
from project.models import Ticker, PriceHistory, Insider, Trade
from marshmallow import fields, Schema
from marshmallow.compat import iteritems
from flask_marshmallow.fields import URLFor


_context_pattern = re.compile(r'^ctx\(\s*(\S*)\s*\)\s*')


def _key(val):
    """Return value within ``( )`` if possible, else return ``None``."""
    match = _context_pattern.match(val)
    if match:
        return match.groups()[0]
    return None


class CustomURLFor(URLFor):
    def _serialize(self, value, attr, obj):

        for name, value in iteritems(self.params):
            attr_context = _key(str(value))
            if attr_context:
                self.params[name] = self.context.get(attr_context, '')
        return super()._serialize(value, attr, obj)


class TickSchema(ma.ModelSchema):
    url = ma.AbsoluteUrlFor('api.get_tick_prices', ticker_name='<name>')

    class Meta:
        model = Ticker
        fields = ('name', 'url')


class PricesTickSchema(ma.ModelSchema):
    class Meta:
        model = PriceHistory
        fields = ('date', 'open', 'high', 'open', 'low', 'close', 'volume')


# class PricesDeltaSchema(PricesTickSchema):
#     g_num = fields.Int()
#     total_diff = fields.Decimal(attribute='diff', rounding=2, as_string=True)
#
#     class Meta:
#         model = PriceHistory
#         fields = ('date', 'open', 'high', 'open', 'low', 'close', 'volume', 'g_num', 'total_diff')


class PricesDeltaSchema(Schema):
    date = fields.DateTime()
    open = fields.String()
    high = fields.String()
    low = fields.String()
    close = fields.String()
    volume = fields.String()
    total_diff = fields.String()


class DeltaListSchema(PricesTickSchema):
    g_num = fields.Int()
    total_diff = fields.Decimal(attribute='diff', rounding=2, as_string=True)

    class Meta:
        model = PriceHistory
        fields = ('date', 'open', 'high', 'low', 'close', 'volume', 'g_num', 'total_diff')


class InsiderSchema(ma.ModelSchema):
    url = CustomURLFor('api.get_insider_trades', ticker_name='ctx(ticker_name)', insider_name='<name>', _external=True)

    class Meta:
        model = Insider
        fields = ('name', 'url')


class InsiderTradeSchema(ma.ModelSchema):
    transaction_type = fields.Function(lambda obj: obj.transaction_type.name)

    class Meta:
        model = Trade
        # fields = ('date', 'open', 'high', 'open', 'low', 'close', 'volume')


class DateParsing(fields.Field):
    """Field that deserializes to a date type into string.
    """
    def _deserialize(self, value, attr, data, **kwargs):
        date_pattern = re.compile(r'(\d{2}).(\d{2}).(\d{4})')
        match = date_pattern.match(value)
        return '-'.join(match.groups())


class AnalyticsPriceSchema(ma.Schema):
    date_from = DateParsing()
    date_to = DateParsing()
    ticker_name = fields.Str()


class DeltaPriceSchema(ma.Schema):
    ticker_name = fields.Str()
    value = fields.Int()
    type = fields.Str(attribute="type_price")
