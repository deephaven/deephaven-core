---
title: Build a Real-Time Trading Dashboard
sidebar_label: Real-Time Trading Dashboard
---

This guide demonstrates how to build a comprehensive real-time trading dashboard using Deephaven's powerful streaming data capabilities. We'll create a simulated trading environment that showcases multiple real-time features including market data feeds, portfolio tracking, risk monitoring, and dynamic analytics.

## Overview

Our real-time trading dashboard will include:

- **Live Market Data**: Simulated stock prices with realistic volatility
- **Portfolio Tracking**: Real-time P&L calculations and position monitoring
- **Risk Analytics**: Dynamic risk metrics and alerts
- **Trade Execution**: Simulated order flow and execution tracking
- **Performance Metrics**: Rolling statistics and performance indicators

## Setting Up the Market Data Feed

First, let's create a realistic market data simulator that generates stock prices with proper volatility patterns:

```python ticking-table order=market_data
from deephaven import time_table, empty_table, new_table
from deephaven import function_generated_table
from deephaven import column as dhcol
import random
import math

# Define our universe of stocks
STOCKS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "NFLX", "AMD", "CRM"]

# Initial stock prices
INITIAL_PRICES = {
    "AAPL": 175.50, "GOOGL": 2800.25, "MSFT": 415.75, "AMZN": 3200.80,
    "TSLA": 245.30, "NVDA": 875.40, "META": 485.60, "NFLX": 425.90,
    "AMD": 165.25, "CRM": 245.75
}

# Store current prices globally
current_prices = INITIAL_PRICES.copy()

def generate_market_data():
    """Generate realistic market data with volatility clustering"""
    symbols = []
    prices = []
    volumes = []
    bid_prices = []
    ask_prices = []
    
    for symbol in STOCKS:
        # Generate price movement with mean reversion and volatility clustering
        volatility = random.uniform(0.001, 0.003)  # 0.1% to 0.3% per tick
        price_change = random.gauss(0, volatility)
        
        # Add some mean reversion
        if current_prices[symbol] > INITIAL_PRICES[symbol] * 1.05:
            price_change -= 0.001
        elif current_prices[symbol] < INITIAL_PRICES[symbol] * 0.95:
            price_change += 0.001
            
        current_prices[symbol] *= (1 + price_change)
        
        # Generate bid-ask spread (0.01% to 0.05%)
        spread_pct = random.uniform(0.0001, 0.0005)
        spread = current_prices[symbol] * spread_pct
        
        symbols.append(symbol)
        prices.append(round(current_prices[symbol], 2))
        volumes.append(random.randint(1000, 50000))
        bid_prices.append(round(current_prices[symbol] - spread/2, 2))
        ask_prices.append(round(current_prices[symbol] + spread/2, 2))
    
    return new_table([
        dhcol.string_col("Symbol", symbols),
        dhcol.double_col("Price", prices),
        dhcol.int_col("Volume", volumes),
        dhcol.double_col("BidPrice", bid_prices),
        dhcol.double_col("AskPrice", ask_prices),
    ])

# Create market data feed that updates every 100ms
market_tick = time_table("PT0.1S")
market_data = function_generated_table(
    table_generator=generate_market_data,
    source_tables=market_tick
)
```

## Portfolio and Position Tracking

Now let's create a portfolio management system that tracks positions and calculates real-time P&L:

```python ticking-table order=portfolio_tracker
# Define initial portfolio positions
INITIAL_POSITIONS = {
    "AAPL": 1000, "GOOGL": 200, "MSFT": 800, "AMZN": 150,
    "TSLA": 500, "NVDA": 300, "META": 400, "NFLX": 250
}

def create_portfolio():
    """Create initial portfolio table"""
    symbols = list(INITIAL_POSITIONS.keys())
    quantities = list(INITIAL_POSITIONS.values())
    avg_costs = [INITIAL_PRICES[symbol] for symbol in symbols]
    
    return new_table([
        dhcol.string_col("Symbol", symbols),
        dhcol.int_col("Quantity", quantities),
        dhcol.double_col("AvgCost", avg_costs),
    ])

# Create static portfolio table
portfolio = create_portfolio()

# Join with live market data to get real-time valuations
portfolio_live = portfolio.natural_join(
    market_data, on="Symbol"
).update([
    "MarketValue = Quantity * Price",
    "CostBasis = Quantity * AvgCost", 
    "UnrealizedPnL = MarketValue - CostBasis",
    "PnLPercent = (UnrealizedPnL / CostBasis) * 100"
])

# Calculate portfolio totals
portfolio_summary = portfolio_live.agg_by([
    "TotalMarketValue = sum(MarketValue)",
    "TotalCostBasis = sum(CostBasis)",
    "TotalUnrealizedPnL = sum(UnrealizedPnL)",
    "AvgPnLPercent = avg(PnLPercent)"
])
```

## Trade Execution Simulator

Let's add a realistic trade execution system that generates random trades:

```python ticking-table order=trade_execution
import uuid
from datetime import datetime

# Trade execution state
trade_counter = 0

def generate_trades():
    """Generate random trade executions"""
    global trade_counter
    
    # Generate 1-5 trades per execution
    num_trades = random.randint(1, 5)
    
    trade_ids = []
    symbols = []
    sides = []
    quantities = []
    prices = []
    timestamps = []
    
    for _ in range(num_trades):
        trade_counter += 1
        symbol = random.choice(STOCKS)
        side = random.choice(["BUY", "SELL"])
        quantity = random.randint(10, 1000)
        
        # Use current market price with some slippage
        base_price = current_prices[symbol]
        slippage = random.uniform(-0.002, 0.002)  # ±0.2% slippage
        execution_price = base_price * (1 + slippage)
        
        trade_ids.append(f"TRD_{trade_counter:06d}")
        symbols.append(symbol)
        sides.append(side)
        quantities.append(quantity)
        prices.append(round(execution_price, 2))
        timestamps.append(datetime.now())
    
    return new_table([
        dhcol.string_col("TradeId", trade_ids),
        dhcol.string_col("Symbol", symbols),
        dhcol.string_col("Side", sides),
        dhcol.int_col("Quantity", quantities),
        dhcol.double_col("Price", prices),
        dhcol.datetime_col("Timestamp", timestamps),
    ])

# Create trade execution feed
trade_tick = time_table("PT2S")  # New trades every 2 seconds
trades = function_generated_table(
    table_generator=generate_trades,
    source_tables=trade_tick
)

# Calculate trade statistics
trade_stats = trades.update([
    "Notional = Quantity * Price"
]).agg_by([
    "TradeCount = count()",
    "TotalNotional = sum(Notional)",
    "AvgTradeSize = avg(Notional)",
    "BuyNotional = sum(Notional * (Side == `BUY` ? 1 : 0))",
    "SellNotional = sum(Notional * (Side == `SELL` ? 1 : 0))"
], by="Symbol")
```

## Risk Analytics and Monitoring

Now let's add sophisticated risk analytics that monitor portfolio risk in real-time:

```python ticking-table order=risk_analytics
def calculate_portfolio_risk():
    """Calculate real-time risk metrics"""
    # Get latest portfolio data
    latest_portfolio = portfolio_live.tail(len(INITIAL_POSITIONS))
    
    # Calculate position concentrations
    total_value = latest_portfolio.sum_by("MarketValue").select("MarketValue").to_numpy().flatten()[0]
    
    symbols = []
    concentrations = []
    var_contributions = []
    
    for symbol in STOCKS:
        if symbol in INITIAL_POSITIONS:
            position_value = INITIAL_POSITIONS[symbol] * current_prices[symbol]
            concentration = (position_value / total_value) * 100
            
            # Simple VaR contribution (simplified for demo)
            volatility = random.uniform(15, 35)  # Annual volatility %
            var_contrib = concentration * volatility / 100
            
            symbols.append(symbol)
            concentrations.append(round(concentration, 2))
            var_contributions.append(round(var_contrib, 2))
    
    return new_table([
        dhcol.string_col("Symbol", symbols),
        dhcol.double_col("Concentration", concentrations),
        dhcol.double_col("VaRContribution", var_contributions),
    ])

# Create risk monitoring feed
risk_tick = time_table("PT5S")  # Update risk every 5 seconds
risk_metrics = function_generated_table(
    table_generator=calculate_portfolio_risk,
    source_tables=risk_tick
)

# Risk alerts - positions over 15% concentration
risk_alerts = risk_metrics.where("Concentration > 15.0").update([
    "AlertType = `CONCENTRATION_RISK`",
    "AlertMessage = `Position ` + Symbol + ` exceeds 15% concentration at ` + Concentration + `%`"
])
```

## Performance Analytics

Let's add rolling performance metrics and analytics:

```python ticking-table order=performance_analytics
# Calculate rolling returns and statistics
market_data_with_returns = market_data.update([
    "PrevPrice = Price_[i-1]",
    "Return = (Price - PrevPrice) / PrevPrice * 100"
]).where("PrevPrice != null")

# Rolling 20-period statistics
rolling_stats = market_data_with_returns.update_by([
    "AvgReturn_20 = rolling_avg_tick(Return, 20)",
    "StdReturn_20 = rolling_std_tick(Return, 20)",
    "MinPrice_20 = rolling_min_tick(Price, 20)",
    "MaxPrice_20 = rolling_max_tick(Price, 20)"
], by="Symbol")

# Calculate Sharpe ratio approximation (simplified)
sharpe_metrics = rolling_stats.update([
    "SharpeRatio = AvgReturn_20 / StdReturn_20",
    "PriceRange = MaxPrice_20 - MinPrice_20",
    "RangePercent = (PriceRange / MinPrice_20) * 100"
]).where("StdReturn_20 > 0")
```

## Market Sentiment Indicator

Finally, let's create a market sentiment indicator based on price movements:

```python ticking-table order=market_sentiment
def calculate_market_sentiment():
    """Calculate overall market sentiment"""
    up_moves = 0
    down_moves = 0
    total_volume = 0
    
    for symbol in STOCKS:
        if symbol in current_prices and symbol in INITIAL_PRICES:
            if current_prices[symbol] > INITIAL_PRICES[symbol]:
                up_moves += 1
            else:
                down_moves += 1
            
            # Add some volume weighting
            total_volume += random.randint(10000, 100000)
    
    sentiment_score = (up_moves - down_moves) / len(STOCKS) * 100
    
    if sentiment_score > 20:
        sentiment = "BULLISH"
    elif sentiment_score < -20:
        sentiment = "BEARISH"
    else:
        sentiment = "NEUTRAL"
    
    return new_table([
        dhcol.double_col("SentimentScore", [sentiment_score]),
        dhcol.string_col("MarketSentiment", [sentiment]),
        dhcol.int_col("UpMoves", [up_moves]),
        dhcol.int_col("DownMoves", [down_moves]),
        dhcol.int_col("TotalVolume", [total_volume]),
    ])

# Market sentiment updates every 10 seconds
sentiment_tick = time_table("PT10S")
market_sentiment = function_generated_table(
    table_generator=calculate_market_sentiment,
    source_tables=sentiment_tick
)
```

## Dashboard Summary

Now you have a complete real-time trading dashboard with multiple interconnected components:

1. **`market_data`** - Live streaming market data with realistic price movements
2. **`portfolio_live`** - Real-time portfolio valuations and P&L
3. **`portfolio_summary`** - Aggregated portfolio metrics
4. **`trades`** - Simulated trade execution feed
5. **`trade_stats`** - Trade analytics by symbol
6. **`risk_metrics`** - Real-time risk monitoring
7. **`risk_alerts`** - Automated risk alerts
8. **`rolling_stats`** - Rolling performance statistics
9. **`sharpe_metrics`** - Risk-adjusted performance metrics
10. **`market_sentiment`** - Overall market sentiment indicator

## Key Features Demonstrated

This dashboard showcases several powerful Deephaven capabilities:

- **Real-time data generation** using `function_generated_table`
- **Streaming joins** between live market data and static portfolio data
- **Dynamic aggregations** for portfolio and trade statistics
- **Rolling window calculations** for performance analytics
- **Complex event processing** for risk monitoring and alerts
- **Multi-table coordination** with different update frequencies

The beauty of Deephaven is that all these tables update automatically as new data arrives, providing a truly real-time view of your trading operations. You can visualize any of these tables in the UI, create custom plots, or build additional analytics on top of this foundation.

## Next Steps

Try modifying the code to:

- Add more sophisticated risk models
- Implement different trading strategies
- Create custom alerts and notifications
- Build additional performance metrics
- Integrate with external data sources

This example demonstrates how Deephaven makes it easy to build complex, real-time analytical applications with just a few lines of Python code!
