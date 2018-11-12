from project import db
from project.api.queries import DELTA_SELECT

from sqlalchemy import event, text, and_
from sqlalchemy.orm import aliased
from sqlalchemy.sql import func, label
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.ext.hybrid import hybrid_property

from babel import Locale
from babel.numbers import parse_decimal

LOCALE = Locale('en_US')


class Ticker(db.Model):

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, unique=True, nullable=False)
    prices = db.relationship('PriceHistory', backref='ticker', lazy=True)
    trades = db.relationship('Trade', backref='ticker', lazy=True)

    def __repr__(self):
        return '<name {}'.format(self.name)


class PriceHistory(db.Model):

    __table_args__ = (
        UniqueConstraint('ticker_id', 'date', name='_ticker__date'),
    )

    id = db.Column(db.Integer, primary_key=True)
    ticker_id = db.Column(db.Integer, db.ForeignKey('ticker.id'), nullable=False)
    date = db.Column(db.Date, nullable=False, server_default=func.current_date())
    open = db.Column(db.Float(decimal_return_scale=2), nullable=False)
    close = db.Column(db.Float(decimal_return_scale=2), nullable=False)
    high = db.Column(db.Float(decimal_return_scale=2), nullable=False)
    low = db.Column(db.Float(decimal_return_scale=2), nullable=False)
    volume = db.Column(db.Integer, nullable=False)

    @classmethod
    def get_or_create(cls, ticker=None, date=None, **kwargs):
        instance = cls.query.filter_by(ticker_id=ticker.id, date=date).first()
        if instance:
            return instance, False
        else:
            kwargs['volume'] = kwargs['volume'].replace(',', '')
            instance = cls(ticker=ticker, date=date, **kwargs)
            db.session.add(instance)
            return instance, True

    def update(self, commit=False, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

        if commit:
            db.session.commit()

    @classmethod
    def get_analytics(cls, ticker_name, date_from, date_to):
        PricesA = aliased(cls)
        PricesB = aliased(cls)

        result = db.session.query(
            label('open', func.abs(PricesA.open - PricesB.open)),
            label('close', func.abs(PricesA.close - PricesB.close)),
            label('low', func.abs(PricesA.low - PricesB.low)),
            label('high', func.abs(PricesA.close - PricesB.high))
        ).join(
            Ticker,
        ).join(
            PricesB,
            and_(PricesB.ticker_id == Ticker.id)
        ).filter(
            Ticker.name == ticker_name,
            PricesA.date == date_from,
            PricesB.date == date_to
        )
        return result

    @classmethod
    def get_delta(cls, ticker_name, type_price, value):
        sql = text(DELTA_SELECT.format(ticker_name=ticker_name, type_price=type_price, value_delta=value))
        result = db.engine.execute(sql)
        return result.fetchall()

    def __repr__(self):
        return '<{} = {}, {}'.format(self.ticker.name, self.volume, self.date)


@event.listens_for(PriceHistory, 'before_insert')
@event.listens_for(PriceHistory, 'before_update')
def serialize_prices_before_puts(mapper, connection, target):
    target.volume = int(
        parse_decimal(target.volume, locale=LOCALE)
    )


class Insider(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.Integer, unique=True)
    name = db.Column(db.String, nullable=False)
    trades = db.relationship('Trade', backref='insider', lazy=True)

    @hybrid_property
    def url_name(self):
        return '-'.join([self.name.lower().replace(' ', '-'), str(self.code)])

    def __repr__(self):
        return '<name {}'.format(self.name)


class TransactionType(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, nullable=False)
    trades = db.relationship('Trade', backref='transaction_type', lazy=True)

    def __repr__(self):
        return '<name {}'.format(self.name)


class Trade(db.Model):

    __table_args__ = (
        UniqueConstraint('ticker_id', 'insider_id', 'transaction_type_id', 'last_date', name='_insider__last_date'),
    )

    id = db.Column(db.Integer, primary_key=True)
    ticker_id = db.Column(db.Integer, db.ForeignKey('ticker.id'), nullable=False)
    insider_id = db.Column(db.Integer, db.ForeignKey('insider.id'), nullable=False)
    transaction_type_id = db.Column(db.Integer, db.ForeignKey('transaction_type.id'), nullable=False)
    shares_traded = db.Column(db.Float(decimal_return_scale=3), nullable=False)
    shares_held = db.Column(db.Float(decimal_return_scale=3), nullable=False)
    last_price = db.Column(db.Float(decimal_return_scale=4), nullable=False)
    last_date = db.Column(db.Date, nullable=False)

    @classmethod
    def get_or_create(cls, ticker=None, insider=None, last_date=None, transaction_type=None, **kwargs):
        instance = cls.query.filter_by(
            ticker=ticker,
            insider_id=insider.id,
            last_date=last_date,
            transaction_type=transaction_type
        ).first()

        if instance:
            return instance, False
        else:
            instance = cls(
                ticker=ticker,
                insider=insider,
                last_date=last_date,
                transaction_type=transaction_type,
                **kwargs
            )
            db.session.add(instance)
            return instance, True

    def update(self, commit=False, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

        if commit:
            db.session.commit()

    def __repr__(self):
        return '<{} = {}, {}'.format(self.insider.name, self.last_price, self.last_date)


@event.listens_for(Trade, 'before_insert')
@event.listens_for(Trade, 'before_update')
def serialize_trade_before_puts(mapper, connection, target):
    target.shares_traded = int(
        parse_decimal(target.shares_traded, locale=LOCALE)
    )
    target.shares_held = int(
        parse_decimal(target.shares_held, locale=LOCALE)
    )

    if not len(target.last_price):
        target.last_price = '0.0'
    target.last_price = float(
        parse_decimal(target.last_price, locale=LOCALE)
    )
