from FMPapi.fmp_api import FMP

# Base object from FMP class
fmp = FMP(symbol="MSFT", silent=False)

# ----------------------------------------------------------------------------- #
# Basics
# ----------------------------------------------------------------------------- #
# general request
req1 = fmp.general_request("/v3/historical-price-full/AAPL?from=2000-03-12&to=2019-03-01")  # working
fmp.general_request("/v4/financial-reports-json?symbol=ALO.PA&year=2020&period=Q1")  # not working example
# all data available for the symbol
dict_data1 = fmp.get_all_data()
dict_data2 = fmp.get_all_data(symbol="MSFT", type="auto")
dict_data3 = fmp.get_all_data(symbol="1248.HK", type="annually")

# ----------------------------------------------------------------------------- #
# Fundamentals
# ----------------------------------------------------------------------------- #
dict_info = fmp.get_stock_info()
df1 = fmp.get_prices_history_daily(symbol=None)  # all available data
df2 = fmp.get_prices_history_daily(symbol="^TYX")
df3 = fmp.get_prices_history_daily(symbol="USDDKK")
df4 = fmp.get_prices_history_daily(symbol=None, date_start=None, date_end=None)  # 5 year data
df5 = fmp.get_income_statements(symbol="MSFT")
df6 = fmp.get_balance_sheet_statements()
df7 = fmp.get_cash_flow_statements(type="auto")
df8 = fmp.get_stock_split_history(symbol="MSFT")

# ----------------------------------------------------------------------------- #
# Analyses
# ----------------------------------------------------------------------------- #
df9 = fmp.get_financial_ratios(symbol="MSFT", type="quarterly")
df10 = fmp.get_financial_ratios(symbol=None, type="auto")
df11 = fmp.get_enterprise_value()
df12 = fmp.get_fmp_company_rating()
df13 = fmp.get_key_metrics()
df14 = fmp.get_discounted_cashflow()

# ----------------------------------------------------------------------------- #
# Random symbols, market indices and ETF symbols:
# ----------------------------------------------------------------------------- #
symbols1 = fmp.get_market_index_symbols("nasdaq100")
symbols2 = fmp.get_symbol()
symbols3 = fmp.get_symbol(search_parameters={"sector": "Technology"}, n=10)
symbols4 = fmp.get_random_symbol()
dict_sectors = fmp.get_symbols_per_sector(return_counts=False)
df15 = fmp.get_symbols_per_sector(return_counts=True)

# ----------------------------------------------------------------------------- #
# Macroeconomic data
# ----------------------------------------------------------------------------- #
# US financial indicators
df16 = fmp.get_financial_indicators_US()
# US housing index
df17 = fmp.get_housing_indicators_US()

# ----------------------------------------------------------------------------- #
# Miscellaneous
# ----------------------------------------------------------------------------- #
df18 = fmp.get_upgrades_downgrades()
df19 = fmp.get_historical_insider_trade_symbol(symbol="AAPL", num_pages=1)

