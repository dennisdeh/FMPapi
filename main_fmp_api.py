import pandas as pd
from fmp_api import FMP

# Base object
fmp = FMP(symbol="MSFT")
# general request
fmp.general_request("/v4/financial-reports-json?symbol=ALO.PA&year=2020&period=Q1")  # not working example
fmp.general_request("/v3/historical-price-full/AAPL?from=2000-03-12&to=2019-03-01")  # working example

# ----------------------------------------------------------------------------- #
# Stock fundamentals
# ----------------------------------------------------------------------------- #
fmp.get_stock_info()
fmp.get_prices_history_daily(symbol=None)  # all available data
fmp.get_prices_history_daily(symbol="^TYX", type="index")
fmp.get_prices_history_daily(symbol="USDDKK", type="currency")
fmp.get_prices_history_daily(symbol=None, date_start=None, date_end=None)  # 5 year data
df = fmp.get_income_statements(symbol="MSFT")
fmp.get_balance_sheet_statements()
fmp.get_cash_flow_statements(type="auto")
fmp.get_stock_split_history(symbol="MSFT")

# ----------------------------------------------------------------------------- #
# Stock fundamental analyses
# ----------------------------------------------------------------------------- #
fmp.get_financial_ratios(symbol="MSFT", type="quarterly")
fmp.get_financial_ratios(symbol=None, type="auto")
fmp.get_enterprise_value()
fmp.get_fmp_company_rating()
fmp.get_key_metrics()
fmp.get_discounted_cashflow()

# ----------------------------------------------------------------------------- #
# Upgrades/downgrades
# ----------------------------------------------------------------------------- #
fmp.get_upgrades_downgrades()

# ----------------------------------------------------------------------------- #
# Insider trading
# ----------------------------------------------------------------------------- #
fmp.get_historical_insider_trade_symbol(symbol="AAPL", num_pages=10)

# ----------------------------------------------------------------------------- #
# Macroeconomic
# ----------------------------------------------------------------------------- #
# US Economic indicators
fmp.get_economic_indicators_US()
# Housing index

# ----------------------------------------------------------------------------- #
# Market indexes and ETFs
# ----------------------------------------------------------------------------- #
