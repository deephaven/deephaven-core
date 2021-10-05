quotes_stream = get_quotes_stream()
quotes_latest = get_quotes_latest(quotes_stream)
usd_prices = get_usd_prices(quotes_latest)

trades_stream = get_trades_stream()
trades_latest = get_trades_latest(trades_stream)
trades_summary = get_trades_summary(trades_stream, usd_prices)