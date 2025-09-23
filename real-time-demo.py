#!/usr/bin/env python3
"""
Deephaven Real-Time Trading Dashboard Demo

This script demonstrates Deephaven's real-time streaming capabilities by creating
a simulated trading environment with live market data, portfolio tracking, 
risk analytics, and performance metrics.

Run this script in a Deephaven environment to see real-time analytics in action!
"""

from deephaven import time_table, empty_table, new_table
from deephaven import function_generated_table
from deephaven import column as dhcol
import random
import math
import uuid
from datetime import datetime

print("🚀 Initializing Deephaven Real-Time Trading Dashboard...")

# =============================================================================
# CONFIGURATION
# =============================================================================

# Stock universe for our simulation
STOCKS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "NFLX", "AMD", "CRM"]

# Initial stock prices (realistic as of 2024)
INITIAL_PRICES = {
    "AAPL": 175.50, "GOOGL": 2800.25, "MSFT": 415.75, "AMZN": 3200.80,
    "TSLA": 245.30, "NVDA": 875.40, "META": 485.60, "NFLX": 425.90,
    "AMD": 165.25, "CRM": 245.75
}

# Initial portfolio positions
INITIAL_POSITIONS = {
    "AAPL": 1000, "GOOGL": 200, "MSFT": 800, "AMZN": 150,
    "TSLA": 500, "NVDA": 300, "META": 400, "NFLX": 250
}

# Global state for price simulation
current_prices = INITIAL_PRICES.copy()
trade_counter = 0

print(f"📊 Configured {len(STOCKS)} stocks and {len(INITIAL_POSITIONS)} portfolio positions")

# =============================================================================
# MARKET DATA SIMULATION
# =============================================================================

def generate_market_data():
    """Generate realistic market data with volatility clustering and mean reversion"""
    symbols = []
    prices = []
    volumes = []
    bid_prices = []
    ask_prices = []
    
    for symbol in STOCKS:
        # Generate price movement with realistic volatility (0.1% to 0.3% per tick)
        volatility = random.uniform(0.001, 0.003)
        price_change = random.gauss(0, volatility)
        
        # Add mean reversion - prices tend to revert to initial levels
        if current_prices[symbol] > INITIAL_PRICES[symbol] * 1.05:
            price_change -= 0.001  # Downward pressure
        elif current_prices[symbol] < INITIAL_PRICES[symbol] * 0.95:
            price_change += 0.001  # Upward pressure
            
        # Update current price
        current_prices[symbol] *= (1 + price_change)
        
        # Generate realistic bid-ask spread (0.01% to 0.05%)
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

print("📈 Creating live market data feed (updates every 100ms)...")

# Create market data feed that updates every 100ms for high-frequency simulation
market_tick = time_table("PT0.1S")
market_data = function_generated_table(
    table_generator=generate_market_data,
    source_tables=market_tick
)

# Add technical indicators to market data
market_data_enhanced = market_data.update([
    "Spread = AskPrice - BidPrice",
    "SpreadBps = (Spread / Price) * 10000",  # Spread in basis points
    "PrevPrice = Price_[i-1]",
    "Return = (Price - PrevPrice) / PrevPrice * 100"
]).where("PrevPrice != null")

print("✅ Market data feed created with technical indicators")

# =============================================================================
# PORTFOLIO MANAGEMENT
# =============================================================================

def create_portfolio():
    """Create initial portfolio table with positions and average costs"""
    symbols = list(INITIAL_POSITIONS.keys())
    quantities = list(INITIAL_POSITIONS.values())
    avg_costs = [INITIAL_PRICES[symbol] for symbol in symbols]
    
    return new_table([
        dhcol.string_col("Symbol", symbols),
        dhcol.int_col("Quantity", quantities),
        dhcol.double_col("AvgCost", avg_costs),
    ])

print("💼 Setting up portfolio tracking...")

# Create static portfolio table
portfolio = create_portfolio()

# Join with live market data to get real-time valuations
portfolio_live = portfolio.natural_join(
    market_data, on="Symbol"
).update([
    "MarketValue = Quantity * Price",
    "CostBasis = Quantity * AvgCost", 
    "UnrealizedPnL = MarketValue - CostBasis",
    "PnLPercent = (UnrealizedPnL / CostBasis) * 100",
    "DailyPnL = Quantity * (Price - AvgCost)"  # Simplified daily P&L
])

# Calculate portfolio-level aggregations
portfolio_summary = portfolio_live.agg_by([
    "TotalMarketValue = sum(MarketValue)",
    "TotalCostBasis = sum(CostBasis)",
    "TotalUnrealizedPnL = sum(UnrealizedPnL)",
    "AvgPnLPercent = avg(PnLPercent)",
    "PositionCount = count()"
])

print("✅ Portfolio tracking configured with real-time P&L calculations")

# =============================================================================
# TRADE EXECUTION SIMULATION
# =============================================================================

def generate_trades():
    """Generate random trade executions with realistic characteristics"""
    global trade_counter
    
    # Generate 1-5 trades per execution cycle
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
        
        # Use current market price with realistic slippage
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

print("⚡ Creating trade execution simulator...")

# Create trade execution feed (new trades every 2 seconds)
trade_tick = time_table("PT2S")
trades = function_generated_table(
    table_generator=generate_trades,
    source_tables=trade_tick
)

# Calculate trade statistics by symbol
trade_stats = trades.update([
    "Notional = Quantity * Price"
]).agg_by([
    "TradeCount = count()",
    "TotalNotional = sum(Notional)",
    "AvgTradeSize = avg(Notional)",
    "BuyNotional = sum(Notional * (Side == `BUY` ? 1 : 0))",
    "SellNotional = sum(Notional * (Side == `SELL` ? 1 : 0))",
    "NetNotional = BuyNotional - SellNotional"
], by="Symbol")

print("✅ Trade execution and statistics configured")

# =============================================================================
# RISK ANALYTICS
# =============================================================================

def calculate_portfolio_risk():
    """Calculate real-time risk metrics including concentrations and VaR"""
    # Calculate total portfolio value for concentration analysis
    total_value = sum(INITIAL_POSITIONS[symbol] * current_prices[symbol] 
                     for symbol in INITIAL_POSITIONS)
    
    symbols = []
    concentrations = []
    var_contributions = []
    position_values = []
    
    for symbol in STOCKS:
        if symbol in INITIAL_POSITIONS:
            position_value = INITIAL_POSITIONS[symbol] * current_prices[symbol]
            concentration = (position_value / total_value) * 100
            
            # Simplified VaR contribution based on concentration and assumed volatility
            assumed_volatility = random.uniform(15, 35)  # Annual volatility %
            var_contrib = concentration * assumed_volatility / 100
            
            symbols.append(symbol)
            concentrations.append(round(concentration, 2))
            var_contributions.append(round(var_contrib, 2))
            position_values.append(round(position_value, 2))
    
    return new_table([
        dhcol.string_col("Symbol", symbols),
        dhcol.double_col("PositionValue", position_values),
        dhcol.double_col("Concentration", concentrations),
        dhcol.double_col("VaRContribution", var_contributions),
    ])

print("🛡️ Setting up risk analytics...")

# Create risk monitoring feed (updates every 5 seconds)
risk_tick = time_table("PT5S")
risk_metrics = function_generated_table(
    table_generator=calculate_portfolio_risk,
    source_tables=risk_tick
)

# Generate risk alerts for high concentrations
risk_alerts = risk_metrics.where("Concentration > 15.0").update([
    "AlertType = `CONCENTRATION_RISK`",
    "AlertLevel = Concentration > 25.0 ? `HIGH` : `MEDIUM`",
    "AlertMessage = `Position ` + Symbol + ` concentration: ` + Concentration + `%`"
])

print("✅ Risk monitoring and alerts configured")

# =============================================================================
# PERFORMANCE ANALYTICS
# =============================================================================

print("📊 Creating performance analytics...")

# Calculate rolling statistics on market data
rolling_stats = market_data_enhanced.update_by([
    "AvgReturn_20 = rolling_avg_tick(Return, 20)",
    "StdReturn_20 = rolling_std_tick(Return, 20)",
    "MinPrice_20 = rolling_min_tick(Price, 20)",
    "MaxPrice_20 = rolling_max_tick(Price, 20)",
    "AvgVolume_20 = rolling_avg_tick(Volume, 20)"
], by="Symbol")

# Calculate advanced performance metrics
performance_metrics = rolling_stats.update([
    "SharpeRatio = StdReturn_20 > 0 ? AvgReturn_20 / StdReturn_20 : null",
    "PriceRange = MaxPrice_20 - MinPrice_20",
    "RangePercent = (PriceRange / MinPrice_20) * 100",
    "Volatility = StdReturn_20",
    "VolumeRatio = Volume / AvgVolume_20"
]).where("StdReturn_20 > 0")

print("✅ Performance analytics with rolling calculations configured")

# =============================================================================
# MARKET SENTIMENT
# =============================================================================

def calculate_market_sentiment():
    """Calculate overall market sentiment based on price movements"""
    up_moves = 0
    down_moves = 0
    total_volume = 0
    total_return = 0
    
    for symbol in STOCKS:
        if symbol in current_prices and symbol in INITIAL_PRICES:
            price_change = (current_prices[symbol] - INITIAL_PRICES[symbol]) / INITIAL_PRICES[symbol]
            total_return += price_change
            
            if current_prices[symbol] > INITIAL_PRICES[symbol]:
                up_moves += 1
            else:
                down_moves += 1
            
            # Add volume weighting for sentiment
            total_volume += random.randint(10000, 100000)
    
    # Calculate sentiment metrics
    sentiment_score = (up_moves - down_moves) / len(STOCKS) * 100
    avg_return = (total_return / len(STOCKS)) * 100
    
    # Determine sentiment category
    if sentiment_score > 20:
        sentiment = "BULLISH"
    elif sentiment_score < -20:
        sentiment = "BEARISH"
    else:
        sentiment = "NEUTRAL"
    
    return new_table([
        dhcol.double_col("SentimentScore", [round(sentiment_score, 2)]),
        dhcol.string_col("MarketSentiment", [sentiment]),
        dhcol.int_col("UpMoves", [up_moves]),
        dhcol.int_col("DownMoves", [down_moves]),
        dhcol.double_col("AvgReturn", [round(avg_return, 2)]),
        dhcol.int_col("TotalVolume", [total_volume]),
    ])

print("🎯 Setting up market sentiment analysis...")

# Market sentiment updates every 10 seconds
sentiment_tick = time_table("PT10S")
market_sentiment = function_generated_table(
    table_generator=calculate_market_sentiment,
    source_tables=sentiment_tick
)

print("✅ Market sentiment indicator configured")

# =============================================================================
# SUMMARY AND INSTRUCTIONS
# =============================================================================

print("\n" + "="*70)
print("🎉 DEEPHAVEN REAL-TIME TRADING DASHBOARD READY!")
print("="*70)
print("\nYour real-time analytics dashboard includes:")
print("\n📊 CORE DATA TABLES:")
print("  • market_data           - Live market prices (100ms updates)")
print("  • market_data_enhanced  - Market data with technical indicators")
print("  • portfolio_live        - Real-time portfolio valuations")
print("  • portfolio_summary     - Aggregated portfolio metrics")
print("  • trades               - Simulated trade executions")
print("  • trade_stats          - Trade analytics by symbol")

print("\n🛡️ RISK & ANALYTICS:")
print("  • risk_metrics         - Position concentrations and VaR")
print("  • risk_alerts          - Automated risk alerts")
print("  • rolling_stats        - Rolling technical indicators")
print("  • performance_metrics  - Sharpe ratios and volatility")
print("  • market_sentiment     - Overall market sentiment")

print("\n🚀 GETTING STARTED:")
print("  1. Open any table in the Deephaven UI to see live updates")
print("  2. Try creating plots: market_data.plot.xy('Symbol', 'Price')")
print("  3. Explore filtering: portfolio_live.where('PnLPercent > 5')")
print("  4. Create custom analytics using the existing tables")

print("\n💡 EXAMPLE QUERIES TO TRY:")
print("  • Top performers: portfolio_live.sort('PnLPercent', order=DESCENDING)")
print("  • High volume stocks: market_data.where('Volume > 30000')")
print("  • Recent trades: trades.tail(10)")
print("  • Risk alerts: risk_alerts (shows concentration warnings)")

print("\n🔄 All tables update automatically in real-time!")
print("   Watch the data flow and see Deephaven's streaming power in action.")
print("\n" + "="*70)

# Optional: Create a dashboard summary table for easy monitoring
dashboard_status = new_table([
    dhcol.string_col("Component", [
        "Market Data Feed", "Portfolio Tracking", "Trade Execution", 
        "Risk Analytics", "Performance Metrics", "Market Sentiment"
    ]),
    dhcol.string_col("Status", ["ACTIVE"] * 6),
    dhcol.string_col("Update_Frequency", [
        "100ms", "Real-time", "2s", "5s", "Real-time", "10s"
    ]),
    dhcol.string_col("Description", [
        "Live stock prices with bid/ask spreads",
        "Real-time P&L calculations",
        "Simulated trade executions",
        "Concentration and VaR monitoring",
        "Rolling statistics and Sharpe ratios",
        "Market sentiment indicator"
    ])
])

print("📋 Dashboard status available in 'dashboard_status' table")
print("\nHappy analyzing! 🚀")
