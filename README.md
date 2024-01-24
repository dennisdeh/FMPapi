### FMPapi
API to work with data from Financial Modeling Prep in a pythonic way.
This little API makes it easy to fetch data for stocks, ETFs, currencies, indexes, and meta data etc. in a convenient format.
Time series are stored as pandas DataFrames.

The following methods are available:
- Basic methods:
  - general_request
  - get_all_data
- Fundamentals:
  - get_income_statements
  - get_balance_sheet_statements 
  - get_cash_flow_statements
  - get_stock_info
  - get_prices_history_daily
  - get_stock_split_history
- Analyses:
  - get_financial_ratios (analyses)
  - get_enterprise_value (analyses)
  - get_key_metrics (analyses)
  - get_fmp_company_rating (analyses)
  - get_discounted_cashflow (analyses)
  - get_ESG_scores (analyses)
  - get_ESG_risk_rating (analyses)
  - get_upgrades_downgrades (analyses)
- Symbols:
  - get_market_index_symbols
  - get_symbol
  - get_random_symbol
  - get_symbols_per_sector
- Macroeconomic data:
  - get_financial_indicators_US
  - get_housing_indicators_US
- Miscellaneous:
  - get_historical_insider_trade_symbol

An API key through a paid plan for FMP is needed to benefit maximally.

See further data points here: https://site.financialmodelingprep.com/developer/docs

This project is not associated with the company 'Financial Modeling Prep' in any way.
