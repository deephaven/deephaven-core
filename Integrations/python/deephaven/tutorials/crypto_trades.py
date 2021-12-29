import random

ct_symbols = ['BTC/USD', 'ETH/USD', 'YFI/USD', 'PAXG/USD']
ct_exchanges = ['binance', 'bitfinex', 'coinbase-pro', 'gemini', 'kraken', 'bitstamp']

ct_x = len(ct_symbols)
ct_y = len(ct_exchanges)

ct_price_map = {'BTC/USD': 55220.00, 'ETH/USD': 3480.00, 'YFI/USD': 30200.00, 'PAXG/USD': 1761.00}


def ct_pricer(instrument, E):
    _aPrice = ct_price_map[instrument]
    from deephaven.conversion_utils import NULL_DOUBLE
    if _aPrice == NULL_DOUBLE:
        ct_price_map[instrument] = abs(E) / 100.0
    else:
        ct_price_map[instrument] = (_aPrice * 100 + (E / 20)) / 100.0
    return ct_price_map[instrument]


def ct_distributor(cnt):
    n = random.randint(0, (cnt * (cnt + 1)) / 2 - 1) + 1
    for i in range(cnt):
        n = n - (cnt - i)
        if n <= 0:
            return int(i)


def ticking_crypto_milliseconds(interval: int):
    from deephaven.TableTools import timeTable
    from deephaven.DateTimeUtils import currentTime, minus
    t = timeTable(minus(currentTime(), 1800000000000), '00:00:00.' + str(interval * 1000).zfill(6)).update(
        'Id=(int)random.randint(12000000,1100000000)', 'B=random.randint(0,1)', 'C=random.randint(0,50)',
        'D= ((int) (random.randint(0,100))/100.0 - 0.5) * 20000.0', 'Instrument=ct_symbols[((int)ct_distributor(ct_x))-1]',
        'Size=(((int) random.randint(1, 100)) / ((int) random.randint(1, 100)))',
        'Price=(double)ct_pricer(Instrument, D)', 'Exchange = ct_exchanges[(int)C%ct_y]',
        'Date = formatDate(Timestamp, TZ_NY)') \
        .dropColumns('B', 'C', 'D') \
        .moveColumnsUp('Date', 'Timestamp', 'Id', 'Instrument', 'Exchange', 'Price', 'Size')
    return t
