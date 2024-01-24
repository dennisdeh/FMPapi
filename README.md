### FMPapi
API to work with data from Financial Modeling Prep in a pythonic way.
This little API makes it easy to fetch data for stocks, ETFs, currencies, indexes, and meta data etc. in a convenient format.
Time series are stored as pandas DataFrames.

The following methods are available:
- general_request
- get_income_statements (fundamentals)
- get_balance_sheet_statements (fundamentals)
- get_cash_flow_statements (fundamentals)
- get_stock_info (fundamentals)
- get_prices_history_daily (fundamentals)
- get_stock_split_history (fundamentals)
- get_financial_ratios (analyses)
- get_enterprise_value (analyses)
- get_key_metrics (analyses)
- get_fmp_company_rating (analyses)
- get_discounted_cashflow (analyses)
- get_ESG_scores (analyses)
- get_ESG_risk_rating (analyses)
- get_upgrades_downgrades (analyses)
- get_historical_insider_trade_symbol
- get_all_data
- get_financial_indicators_US (macroeconomic data)
- get_housing_indicators_US (macroeconomic data)
- get_market_index_symbols (symbols)
- get_symbol (symbols)
- get_random_symbol (symbols)
- get_symbols_per_sector (symbols)

An API key through a paid plan for FMP is needed to benefit maximally.

See further data points here: https://site.financialmodelingprep.com/developer/docs

This project is not associated with the company 'Financial Modeling Prep' in any way.
