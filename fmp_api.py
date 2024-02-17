import pandas as pd
import json
import sys
from typing import Union
from urllib.request import urlopen
import random
import os
import requests
import time
from urllib.error import URLError, HTTPError
from http.client import RemoteDisconnected, IncompleteRead


class FMP:
    def __init__(
        self,
        symbol: Union[str, None] = None,
        key_path: Union[str, None] = None,
        silent: bool = False,
        check_online: bool = False,
        time_wait_retry: int = 10,
        time_wait_query: float = 0.01,
        retries: int = 5,
    ):
        """
        API class for Financial Modeling Prep that implements some
        endpoints available.
        The class can take a default symbol as an input, for which all
        methods will be evaluated, or an explicit symbol.
        General methods for the main data categories are available.

        The API key to FMP must be provided as a .json file and named
        keys.json to work. If no key_path is given, the current working
        directory will be used.

        Parameters
        ----------
        symbol: (str or None)
            The symbol to be used as default for all evaluations.
            If None, it has to be explicitly inserted in each method.
        key_path: (str or None)
            The path to the API json file keys.json, where the key is
            stored under 'financial_modeling_prep'.
        silent: (bool)
            Should informative messages be printed?
        check_online: (bool)
            Check if the server is reachable
        time_wait_retry: (int)
            Time to wait before a retry (in seconds)
        time_wait_query: (float)
            Time to wait before a sending a query (in seconds)
        retries: (int)
            Number of times to try downloading again after a short break
        """
        # Check that keys file exists
        if key_path is None:
            key_path = sys.path[-1]  # if not the working directory,
        elif isinstance(key_path, str):
            pass
        else:
            raise TypeError("Invalid input for key_path. Must a string or None")
        # Load API key
        if os.path.isfile(f"{key_path}\\keys.json"):
            with open(f"{key_path}\\keys.json", "r") as key_file:
                keys = json.load(key_file)
            financial_modeling_prep = keys["financial_modeling_prep"]
        else:
            raise FileNotFoundError(f"Keys file not found at {key_path}")

        # setup
        self.symbol = symbol
        self.key = financial_modeling_prep
        self.silent = silent
        self.time_wait_retry = time_wait_retry
        self.time_wait_query = time_wait_query
        self.retries = retries
        self.api_address = "https://financialmodelingprep.com/api"

        # Check that API is running
        if check_online:
            if requests.get(f"{self.api_address}/v4").status_code == 200:
                if not self.silent:
                    print("FMP API is online")
            else:
                raise ConnectionError("Cannot contact FMP API!")

    # ----------------------------------------------------------------------------- #
    # Helper methods
    # ----------------------------------------------------------------------------- #
    def get_jsonparsed_data(self, url) -> json:
        """
        Receive the content of "url", parse it as JSON and return the object.
        Checks if the returned data is empty or if an error occurred.

        Parameters
        ----------
        url: (str)
            url of the API

        Returns
        -------
        json:
            parsed content returned from the API.
        """
        # get response from API
        # try to download the data (a couple of times)
        n = 0
        while n < self.retries:
            try:
                time.sleep(self.time_wait_query)
                response = urlopen(url)
                break  # if succeeded, break the loop
            except (URLError, RemoteDisconnected, HTTPError, IncompleteRead):
                if not self.silent:
                    print(f"FMP: Trying downloading again (trial {n})... ")
                n += 1
                time.sleep(self.time_wait_retry)
        # step 2.2: assertions
        if n >= self.retries:  # raise exception if no data was found
            raise ConnectionError(
                f"Could not establish connection to the server - Details:\n"
                f"URL: {url}\n"
                f"response (might not be exactly what was returned): {urlopen(url)}"
            )
        # parse data
        data = response.read().decode("utf-8")
        data = json.loads(data)

        # checks
        if len(data) == 0 or (
            isinstance(data, dict) and "Error Message" in data.keys()
        ):
            raise LookupError("Data table not found with FMP API.")

        return data

    def helper_symbol(self, symbol: Union[str, None] = None) -> str:
        """
        Helper function for symbols. Perform checks.
        """
        if isinstance(symbol, str):
            return symbol
        elif symbol is None:
            if isinstance(self.symbol, str):
                return self.symbol
            elif self.symbol is None:
                raise NameError("Neither default or explicit symbol given!")
            else:
                raise ValueError("Invalid input for symbol.")
        else:
            raise ValueError("Invalid input for symbol.")

    # Send general request
    def general_request(self, message: str, API_key_liaison: str = "&") -> json:
        """
        General method for not-yet-implemented endpoints, API key inserted.
        Output parsed as .json.
        API liaison can be changed (necessary for some requests).
        """
        url = f"{self.api_address}/{message}{API_key_liaison}apikey={self.key}"
        return self.get_jsonparsed_data(url)

    # Collect all data available for the symbol
    def get_all_data(self, symbol: Union[str, None] = None, type: str = "auto") -> dict:
        """
        Collects all data for symbols.

        If a symbol does not have a certain type of data,
        the value of the corresponding key will be None.

        Parameters
        ----------
        symbol: (str or None)
            The symbol. If None, the default symbol will be used.
        type: (str)
            "auto" (i.e. try quarterly, else annually), "quarterly" or "annually".

        Returns
        -------
        dict
        """
        # initialise
        symbol = self.helper_symbol(symbol=symbol)
        data = {}
        # prices
        try:
            data["Prices"] = self.get_prices_history_daily(
                symbol=symbol, date_start="1900-01-01", date_end=None
            )
        except LookupError:
            data["Prices"] = None
        # meta data
        try:
            data["Meta data"] = self.get_stock_info(symbol=symbol)
        except LookupError:
            data["Meta data"] = None
        # income
        try:
            data["Income"] = self.get_income_statements(symbol=symbol, type=type)
        except LookupError:
            data["Income"] = None
        # balance
        try:
            data["Balance"] = self.get_balance_sheet_statements(
                symbol=symbol, type=type
            )
        except LookupError:
            data["Balance"] = None
        # cash flow
        try:
            data["Cash flow"] = self.get_cash_flow_statements(symbol=symbol, type=type)
        except LookupError:
            data["Cash flow"] = None
        # financial ratios
        try:
            data["Financial ratios"] = self.get_financial_ratios(
                symbol=symbol, type=type
            )
        except LookupError:
            data["Financial ratios"] = None
        # enterprise value
        try:
            data["Enterprise value"] = self.get_enterprise_value(
                symbol=symbol, type=type
            )
        except LookupError:
            data["Enterprise value"] = None
        # fmp rating
        try:
            data["FMP rating"] = self.get_fmp_company_rating(symbol=symbol)
        except LookupError:
            data["FMP rating"] = None
        # key metrics
        try:
            data["Key metrics"] = self.get_key_metrics(symbol=symbol, type=type)
        except LookupError:
            data["Key metrics"] = None
        # discounted cash flow
        try:
            data["Discounted cashflow"] = self.get_discounted_cashflow(symbol=symbol)
        except LookupError:
            data["Discounted cashflow"] = None

        return data

    # ----------------------------------------------------------------------------- #
    # Fundamentals
    # ----------------------------------------------------------------------------- #
    # Financial statements
    def get_income_statements(
        self, symbol: Union[str, None] = None, type: str = "auto"
    ) -> pd.DataFrame:
        """
        Filed quarterly or annual income statements

        Parameters
        ----------
        symbol: (str or None)
            The symbol. If None, the default symbol will be used.
        type: (str)
            "auto" (try first quarterly, then annually if it fails), "quarterly"
            or "annually".

        Returns
        -------
        df: (pd.DataFrame)
            Output data parsed to a pd.DataFrame object.
        """
        # initial checks and messages
        symbol = self.helper_symbol(symbol=symbol)
        if not self.silent:
            print(f"Fetching income statements {type} history for {symbol}")

        # get data
        if type == "auto":
            try:
                url = f"{self.api_address}/v3/income-statement/{symbol}?period=quarter&apikey={self.key}"
                data = self.get_jsonparsed_data(url)
            except IndexError:
                url = f"{self.api_address}/v3/income-statement/{symbol}?limit=120&apikey={self.key}"
                data = self.get_jsonparsed_data(url)
        elif type == "quarterly":
            url = f"{self.api_address}/v3/income-statement/{symbol}?period=quarter&apikey={self.key}"
            data = self.get_jsonparsed_data(url)
        elif type == "annually":
            url = f"{self.api_address}/v3/income-statement/{symbol}?limit=120&apikey={self.key}"
            data = self.get_jsonparsed_data(url)
        else:
            raise NameError("Invalid input for type.")

        # convert to dataframe, parse date, raise exception if parsing can not be done
        try:
            df = pd.DataFrame(data)
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
            df = df.set_index("date")
        except ValueError:
            raise LookupError("Data table not consistent or of expected format")

        return df

    def get_balance_sheet_statements(
        self, symbol: Union[str, None] = None, type: str = "auto"
    ) -> pd.DataFrame:
        """
        Quarterly or annual balance sheet statements

        Parameters
        ----------
        symbol: (str or None)
            The symbol. If None, the default symbol will be used.
        type: (str)
            "auto" (try first quarterly, then annually if it fails), "quarterly"
            or "annually".

        Returns
        -------
        df: (pd.DataFrame)
            Output data parsed to a pd.DataFrame object.
        """
        # initial checks and messages
        symbol = self.helper_symbol(symbol=symbol)
        if not self.silent:
            print(f"Fetching balance sheet statements {type} history for {symbol}")

        # get data
        if type == "auto":
            try:
                url = f"{self.api_address}/v3/balance-sheet-statement/{symbol}?period=quarter&apikey={self.key}"
                data = self.get_jsonparsed_data(url)
            except LookupError:
                url = f"{self.api_address}/v3/balance-sheet-statement/{symbol}?limit=120&apikey={self.key}"
                data = self.get_jsonparsed_data(url)
        elif type == "quarterly":
            url = f"{self.api_address}/v3/balance-sheet-statement/{symbol}?period=quarter&apikey={self.key}"
            data = self.get_jsonparsed_data(url)
        elif type == "annually":
            url = f"{self.api_address}/v3/balance-sheet-statement/{symbol}?limit=120&apikey={self.key}"
            data = self.get_jsonparsed_data(url)
        else:
            raise NameError("Invalid input for type.")

        # convert to dataframe, parse date, raise exception if parsing can not be done
        try:
            df = pd.DataFrame(data)
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
            df = df.set_index("date")
        except ValueError:
            raise LookupError("Data table not consistent or of expected format")

        return df

    def get_cash_flow_statements(
        self, symbol: Union[str, None] = None, type: str = "auto"
    ) -> pd.DataFrame:
        """
        Quarterly or annual cash flow statements

        Parameters
        ----------
        symbol: (str or None)
            The symbol. If None, the default symbol will be used.
        type: (str)
            "auto" (try first quarterly, then annually if it fails), "quarterly"
            or "annually".

        Returns
        -------
        df: (pd.DataFrame)
            Output data parsed to a pd.DataFrame object.
        """
        # initial checks and messages
        symbol = self.helper_symbol(symbol=symbol)
        if not self.silent:
            print(f"Fetching cash flow statements {type} history for {symbol}")

        # get data
        if type == "auto":
            try:
                url = f"{self.api_address}/v3/cash-flow-statement/{symbol}?period=quarter&apikey={self.key}"
                data = self.get_jsonparsed_data(url)
            except LookupError:
                url = f"{self.api_address}/v3/cash-flow-statement/{symbol}?limit=120&apikey={self.key}"
                data = self.get_jsonparsed_data(url)
        elif type == "quarterly":
            url = f"{self.api_address}/v3/cash-flow-statement/{symbol}?period=quarter&apikey={self.key}"
            data = self.get_jsonparsed_data(url)
        elif type == "annually":
            url = f"{self.api_address}/v3/cash-flow-statement/{symbol}?limit=120&apikey={self.key}"
            data = self.get_jsonparsed_data(url)
        else:
            raise NameError("Invalid input for type.")
        # convert to dataframe, parse date, raise exception if parsing can not be done
        try:
            df = pd.DataFrame(data)
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
            df = df.set_index("date")
        except ValueError:
            raise LookupError("Data table not consistent or of expected format")

        return df

    # Info
    def get_stock_info(self, symbol: Union[str, None] = None) -> dict:
        """
        Fetch stock info and meta-data

        Parameters
        ----------
        symbol: (str or None)
            The symbol. If None, the default symbol will be used.

        Returns
        -------
        df: dict
            Output data parsed as a dict
        """
        # initial checks and messages
        symbol = self.helper_symbol(symbol=symbol)
        if not self.silent:
            print(f"Fetching meta-data for {symbol}")

        url = f"{self.api_address}/v3/profile/{symbol}?apikey={self.key}"

        return self.get_jsonparsed_data(url)[0]

    # Prices
    def get_prices_history_daily(
        self,
        symbol: Union[str, None] = None,
        date_start: Union[str, None] = "1900-01-01",
        date_end: Union[str, None] = None,
    ) -> pd.DataFrame:
        """
        Fetch daily stock/index/currency prices of the symbol (open/high/low/
        close/adj. close), volume, change, etc.
        Per default data starting from "1900-01-01" (i.e. as much as is available
        essentially) is downloaded. If date_start=None, 5 years of data will be
        downloaded.

        Parameters
        ----------
        symbol: (str or None)
            The symbol. If None, the default symbol will be used.
        date_end: str
            Date to start downloading data from. Per default the maximum range is taken.
        date_start: str
            Date to end downloading data to.

        Returns
        -------
        df: (pd.DataFrame)
            Output data parsed to a pd.DataFrame object.
        """
        # initial checks and messages
        symbol = self.helper_symbol(symbol=symbol)
        if not self.silent:
            print(f"Fetching historical daily prices for {symbol}")

        # step 1: prepare period strings
        # e.g. v3/historical-price-full/AAPL?from=2011-03-12&to=2019-03-12
        if date_start is None:  # default is 5 years
            date_str = ""
        else:
            date_str = f"from={date_start}&"
        if date_end is None:
            date_str = date_str
        else:
            date_str = f"{date_str}to={date_end}&"

        # step 2: send request and prepare data frame
        url = f"{self.api_address}/v3/historical-price-full/{symbol}?{date_str}apikey={self.key}"
        prices = self.get_jsonparsed_data(url)
        # convert to dataframe, parse date, raise exception if parsing can not be done
        try:
            df = pd.DataFrame(prices["historical"])
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
            df = df.set_index("date")
        except ValueError:
            raise LookupError("Data table not consistent or of expected format")

        return df

    def get_stock_split_history(self, symbol: Union[str, None] = None) -> pd.DataFrame:
        """
        Fetch stock split history of the symbol

        Parameters
        ----------
        symbol: (str or None)
            The symbol. If None, the default symbol will be used.

        Returns
        -------
        df: (pd.DataFrame)
            Output data parsed to a pd.DataFrame object.
        """
        # initial checks and messages
        symbol = self.helper_symbol(symbol=symbol)
        if not self.silent:
            print(f"Fetching stock split history for {symbol}")

        url = f"{self.api_address}/v3/historical-price-full/stock_split/{symbol}?&apikey={self.key}"
        stock_split = self.get_jsonparsed_data(url)
        # convert to dataframe, parse date, raise exception if parsing can not be done
        try:
            df = pd.DataFrame(stock_split["historical"])
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
            df = df.set_index("date")
        except ValueError:
            raise LookupError("Data table not consistent or of expected format")

        return df

    # ----------------------------------------------------------------------------- #
    # Analyses
    # ----------------------------------------------------------------------------- #
    def get_financial_ratios(
        self, symbol: Union[str, None] = None, type: str = "auto"
    ) -> pd.DataFrame:
        """
        Quarterly or annual company financial ratios (annual if quarterly are not available)

        Parameters
        ----------
        symbol: (str or None)
            The symbol. If None, the default symbol will be used.
        type: (str)
            "auto" (try first quarterly, then annually if it fails), "quarterly"
            or "annually".

        Returns
        -------
        df: (pd.DataFrame)
            Output data parsed to a pd.DataFrame object.
        """
        # initial checks and messages
        symbol = self.helper_symbol(symbol=symbol)
        if not self.silent:
            print(f"Fetching financial ratios {type} history for {symbol}")

        # get data
        if type == "auto":
            try:
                url = f"{self.api_address}/v3/ratios/{symbol}?period=quarter?limit=400&apikey={self.key}"
                data = self.get_jsonparsed_data(url)
            except LookupError:
                url = (
                    f"{self.api_address}/v3/ratios/{symbol}?limit=120&apikey={self.key}"
                )
                data = self.get_jsonparsed_data(url)
        elif type == "quarterly":
            url = f"{self.api_address}/v3/ratios/{symbol}?period=quarter?limit=400&apikey={self.key}"
            data = self.get_jsonparsed_data(url)
        elif type == "annually":
            url = f"{self.api_address}/v3/ratios/{symbol}?limit=120&apikey={self.key}"
            data = self.get_jsonparsed_data(url)
        else:
            raise NameError("Invalid input for type.")
        # convert to dataframe, parse date, raise exception if parsing can not be done
        try:
            df = pd.DataFrame(data)
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
            df = df.set_index("date")
        except ValueError:
            raise LookupError("Data table not consistent or of expected format")

        return df

    def get_enterprise_value(
        self, symbol: Union[str, None] = None, type: str = "quarterly"
    ) -> pd.DataFrame:
        """
        Quarterly or annual company enterprise value (annual if quarterly are not available)

        Parameters
        ----------
        symbol: (str or None)
            The symbol. If None, the default symbol will be used.
        type: (str)
            "quarterly" or "annually".

        Returns
        -------
        df: (pd.DataFrame)
            Output data parsed to a pd.DataFrame object.
        """
        # initial checks and messages
        symbol = self.helper_symbol(symbol=symbol)
        if not self.silent:
            print(f"Fetching enterprise values {type} history for {symbol}")

        # get data
        if type == "auto":
            try:
                url = f"{self.api_address}/v3/enterprise-values/{symbol}?period=quarter?limit=400&apikey={self.key}"
                data = self.get_jsonparsed_data(url)
            except LookupError:
                url = f"{self.api_address}/v3/enterprise-values/{symbol}?limit=120&apikey={self.key}"
                data = self.get_jsonparsed_data(url)
        elif type == "quarterly":
            url = f"{self.api_address}/v3/enterprise-values/{symbol}?period=quarter?limit=400&apikey={self.key}"
            data = self.get_jsonparsed_data(url)
        elif type == "annually":
            url = f"{self.api_address}/v3/enterprise-values/{symbol}?limit=120&apikey={self.key}"
            data = self.get_jsonparsed_data(url)
        else:
            raise NameError("Invalid input for type.")
        # convert to dataframe, parse date, raise exception if parsing can not be done
        try:
            df = pd.DataFrame(data)
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
            df = df.set_index("date")
        except ValueError:
            raise LookupError("Data table not consistent or of expected format")

        return df

    def get_key_metrics(
        self, symbol: Union[str, None] = None, type: str = "quarterly"
    ) -> pd.DataFrame:
        """
        Quarterly or annual company key metrics (annual if quarterly are not available)

        Parameters
        ----------
        symbol: (str or None)
            The symbol. If None, the default symbol will be used.
        type: (str)
            "auto" (i.e. try quarterly, else annually), "quarterly" or "annually".

        Returns
        -------
        df: (pd.DataFrame)
            Output data parsed to a pd.DataFrame object.
        """
        # initial checks and messages
        symbol = self.helper_symbol(symbol=symbol)
        if not self.silent:
            print(f"Fetching key metrics {type} history for {symbol}")

        # get data
        if type == "auto":
            try:
                url = f"{self.api_address}/v3/key-metrics/{symbol}?period=quarter?limit=400&apikey={self.key}"
                data = self.get_jsonparsed_data(url)
            except LookupError:
                url = f"{self.api_address}/v3/key-metrics/{symbol}?limit=120&apikey={self.key}"
                data = self.get_jsonparsed_data(url)
        elif type == "quarterly":
            url = f"{self.api_address}/v3/key-metrics/{symbol}?period=quarter?limit=400&apikey={self.key}"
            data = self.get_jsonparsed_data(url)
        elif type == "annually":
            url = f"{self.api_address}/v3/key-metrics/{symbol}?limit=120&apikey={self.key}"
            data = self.get_jsonparsed_data(url)
        else:
            raise NameError("Invalid input for type.")
        # convert to dataframe, parse date, raise exception if parsing can not be done
        try:
            df = pd.DataFrame(data)
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
            df = df.set_index("date")
        except ValueError:
            raise LookupError("Data table not consistent or of expected format")

        return df

    def get_fmp_company_rating(self, symbol: Union[str, None] = None) -> pd.DataFrame:
        """
        Daily company rating from FPM using their method:
        https://site.financialmodelingprep.com/developer/docs/recommendations-formula/

        Parameters
        ----------
        symbol: (str or None)
            The symbol. If None, the default symbol will be used.

        Returns
        -------
        df: (pd.DataFrame)
            Output data parsed to a pd.DataFrame object.
        """
        # initial checks and messages
        symbol = self.helper_symbol(symbol=symbol)
        if not self.silent:
            print(f"Fetching company rating history for {symbol}")

        url = f"{self.api_address}/v3/historical-rating/{symbol}?limit=50000&apikey={self.key}"
        data = self.get_jsonparsed_data(url)
        # convert to dataframe, parse date, raise exception if parsing can not be done
        try:
            df = pd.DataFrame(data)
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
            df = df.set_index("date")
        except ValueError:
            raise LookupError("Data table not consistent or of expected format")

        return df

    def get_discounted_cashflow(self, symbol: Union[str, None] = None) -> pd.DataFrame:
        """
        Daily discounted cash flow.

        Parameters
        ----------
        symbol: (str or None)
            The symbol. If None, the default symbol will be used.

        Returns
        -------
        df: (pd.DataFrame)
            Output data parsed to a pd.DataFrame object.
        """
        # initial checks and messages
        symbol = self.helper_symbol(symbol=symbol)
        if not self.silent:
            print(f"Fetching company rating history for {symbol}")

        url = f"{self.api_address}/v3/historical-daily-discounted-cash-flow/{symbol}?limit=50000&apikey={self.key}"
        data = self.get_jsonparsed_data(url)
        # convert to dataframe, parse date, raise exception if parsing can not be done
        try:
            df = pd.DataFrame(data)
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
            df = df.set_index("date")
        except ValueError:
            raise LookupError("Data table not consistent or of expected format")

        return df

    # Environmental, social and governance (ESG) scores
    def get_ESG_scores(self, symbol: Union[str, None] = None) -> pd.DataFrame:
        """
        ESG score (mostly quarterly)

        Parameters
        ----------
        symbol: (str or None)
            The symbol. If None, the default symbol will be used.

        Returns
        -------
        df: (pd.DataFrame)
            Output data parsed to a pd.DataFrame object.
        """
        # initial checks and messages
        symbol = self.helper_symbol(symbol=symbol)
        if not self.silent:
            print(f"Fetching ESG score history for {symbol}")

        url = f"{self.api_address}/v4/esg-environmental-social-governance-data?symbol={symbol}&apikey={self.key}"
        data = self.get_jsonparsed_data(url)
        # convert to dataframe, parse date, raise exception if parsing can not be done
        try:
            df = pd.DataFrame(data)
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
            df = df.set_index("date")
        except ValueError:
            raise LookupError("Data table not consistent or of expected format")

        return df

    # Company ESG risk rating
    def get_ESG_risk_rating(self, symbol: Union[str, None] = None) -> pd.DataFrame:
        """
        Annual ESG risk rating

        Parameters
        ----------
        symbol: (str or None)
            The symbol. If None, the default symbol will be used.

        Returns
        -------
        df: (pd.DataFrame)
            Output data parsed to a pd.DataFrame object.
        """
        # initial checks and messages
        symbol = self.helper_symbol(symbol=symbol)
        if not self.silent:
            print(f"Fetching ESG score history for {symbol}")

        url = (
            f"{self.api_address}/v4/esg-environmental-social-governance-data-ratings"
            f"?symbol={symbol}&apikey={self.key}"
        )
        data = self.get_jsonparsed_data(url)
        # convert to dataframe, parse date, raise exception if parsing can not be done
        try:
            df = pd.DataFrame(data)
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
            df = df.set_index("date")
        except ValueError:
            raise LookupError("Data table not consistent or of expected format")

        return df

    # ----------------------------------------------------------------------------- #
    # Random symbols, market indices and ETF symbols
    # ----------------------------------------------------------------------------- #
    def get_market_index_symbols(self, market_index: str):
        """
        This method gets the symbols currently in the given index.

        Parameters
        ----------
        market_index: str
            The market index to get symbols for. Currently, the following are implemented:
                - sp500: Standard and Poor's 500, tracking the stock performance of 500 large companies
                         listed on stock exchanges in the United States.
                - nasdaq100: The Nasdaq-100 (^NDX[2]) is a stock market index made up of 101 equity
                             securities issued by 100 of the largest non-financial companies listed
                             on the Nasdaq stock exchange.
                - dj: The Dow Jones Industrial Average (DJIA), Dow Jones, or simply the Dow, is a
                      stock market index of 30 prominent companies listed on stock exchanges in the
                      United States.
                - euronext: Euronext N.V. (short for European New Exchange Technology) is a pan-European
                            bourse that offers various trading and post-trade services.
                - tsx: Toronto Stock Exchange is a stock exchange located in Toronto, Ontario, Canada.
                - ETFs: List of ETF symbols currently traded

        Returns
        -------
        list:
            list of symbols in the market index.
        """
        # step 1: get dictionary of market indices
        # S&P500
        if market_index == "sp500":
            mi_dict = self.general_request("/v3/sp500_constituent", API_key_liaison="?")
        # Nasdaq100
        elif market_index == "nasdaq100":
            mi_dict = self.general_request(
                "/v3/nasdaq_constituent", API_key_liaison="?"
            )
        # Dow Jones
        elif market_index == "dj":
            mi_dict = self.general_request(
                "/v3/dowjones_constituent", API_key_liaison="?"
            )
        # EuroNext
        elif market_index == "euronext":
            mi_dict = self.general_request(
                "/v3/symbol/available-euronext", API_key_liaison="?"
            )
        # TSX
        elif market_index == "tsx":
            mi_dict = self.general_request(
                "/v3/symbol/available-tsx", API_key_liaison="?"
            )
        # ETFs
        elif market_index == "ETFs":
            mi_dict = self.general_request("/v3/etf/list", API_key_liaison="?")
        else:
            raise ValueError("Invalid market_index")

        # step 2: create list of symbols
        symbols = []
        for x in mi_dict:
            symbols.append(x["symbol"])
        return symbols

    def get_symbol(
        self,
        search_parameters: Union[None, dict] = None,
        n: int = 1,
        reset: bool = False,
        seed: int = 45,
    ):
        """
        Get a list of symbol(s) from FMP given some search parameters:

        search_parameters: None, dict
            Must be in the order given here:
                marketCapMoreThan & marketCapLowerThan : Number
                priceMoreThan & priceLowerThan : Number
                betaMoreThan & betaLowerThan : Number
                volumeMoreThan & volumeLowerThan : Number
                dividendMoreThan & dividendLowerThan : Number
                isEtf & isActivelyTrading : true/false
                sector : Consumer Cyclical, Energy, Technology, Industrials, Financial Services,
                    Basic Materials, Communication Services, Consumer Defensive, Healthcare, Real Estate,
                    Utilities, Industrial Goods, Financial, Services, Conglomerates
                industry : Autos, Banks, Banks Diversified, Software, Banks Regional, Beverages Alcoholic,
                    Beverages Brewers, Beverages Non-Alcoholic
                country : US, UK, MX, BR, RU, HK, CA,
                exchange : nyse, nasdaq, amex, euronext, tsx, etf, mutual_fund
        n: int
            number of symbols to return in the list
        reset: bool
            Whether to reset random seed or not
        seed: int
            random seed
        """
        # case A: Random symbols for stocks with financial statements
        if search_parameters is None:
            # initialise
            url_str = "v3/financial-statement-symbol-lists"
            if reset:
                random.seed(seed)
            # get symbols
            list_symbols = self.general_request(message=url_str, API_key_liaison="?")
            # select the number of symbols requested
            if n is None:
                symbols = list_symbols
            elif isinstance(n, int):
                symbols = random.sample(list_symbols, k=n)
        # case B: Select stocks satisfying certain criteria
        elif isinstance(search_parameters, dict):
            # initialise (number of stocks also put in here)
            url_str = f"v3/stock-screener?limit={n}"
            # prepare list with parsed criteria
            for key in search_parameters:
                url_str = f"{url_str}&{key}={search_parameters[key]}"
            # get meta data (list of dicts)
            list_metadata = self.general_request(message=url_str, API_key_liaison="&")
            # select stock symbols
            symbols = []
            for d in list_metadata:
                symbols.append(d["symbol"])
        else:
            raise ValueError("Invalid input for search_parameters")

        return symbols

    def get_random_symbol(self, n=1, reset: bool = False, seed: int = 45):
        """
        Get a list of random symbol(s) from FMP (selected from those
        with financial statements available)

        Parameters
        ----------
        n: int
            number of symbols to return in the list
        reset: bool
            Whether to reset random seed or not
        seed: int
            random seed
        """
        if reset:
            random.seed(seed)

        # step 2: get list of symbols with financial statements on FMP
        list_symbols = self.general_request(
            message="v3/financial-statement-symbol-lists", API_key_liaison="?"
        )

        return random.sample(list_symbols, k=n)

    def get_symbols_per_sector(self, return_counts: bool = True):
        """
        Get all symbols for all sectors.
        If return_counts is True, a dataframe will be returned with
        the number of stocks per sector. Otherwise, a dict will
        be returned with all the symbols.
        """
        sectors = [
            "Consumer%20Cyclical",
            "Energy",
            "Technology",
            "Industrials",
            "Financial%20Services",
            "Basic%20Materials",
            "Communication%20Services",
            "Consumer%20Defensive",
            "Healthcare",
            "Real%20Estate",
            "Utilities",
            "Industrial%20Goods",
            "Financial",
            "Services",
            "Conglomerates",
        ]

        d = {}
        n = 10**6
        for sector in sectors:
            symbols = self.get_symbol(search_parameters={"sector": sector}, n=n)
            if return_counts:
                d[sector] = len(symbols)
            else:
                d[sector] = symbols

        if return_counts:
            return pd.DataFrame(d, index=["n"]).T
        else:
            return d

    # ----------------------------------------------------------------------------- #
    # Macroeconomic data
    # ----------------------------------------------------------------------------- #
    # Financial indicators
    def get_financial_indicators_US(self):
        """
        Monthly financial indicators for the US. Contains the data listed below:

        # Retail money funds:
        Retail funds are investment funds intended for ordinary investors, as opposed
        to institutional investors.
        Retail funds include many classes of mutual funds and ETFs available for transactions
        through brokers or directly from the fund company.

        # Federal funds:
        Federal funds refer to excess reserves held by financial institutions, over and above
        the mandated reserve requirements of the central bank.
        Banks will borrow or lend their excess funds to each other on an overnight basis, as
        some banks find themselves with too many reserves and others with too little.
        The federal funds rate is a target set by the central bank, but the actual market rate
        for federal fund reserves is determined by this overnight interbank lending market.

        # 3 Months rates and yields certificates of deposit (3M_CD_rate):
        Top-paying certificates of deposit (CDs) pay higher interest rates than the best savings
        and money market accounts in exchange for leaving the funds on deposit for a fixed period
        of time.
        CDs are a safer and more conservative investment than stocks and bonds, offering lower
        opportunity for growth, but with a non-volatile, guaranteed rate of return.
        Virtually every bank, credit union, and brokerage firm offers a menu of CD options.

        # Commercial bank interest rate on credit card plans all accounts (CC_interest_commercial)

        Returns
        -------
        df: (pd.DataFrame)
            Output data parsed to a pd.DataFrame object.
        """
        # initial checks and messages
        if not self.silent:
            print(f"Fetching US financial indicators...")

        # get data
        df = pd.DataFrame(
            self.general_request("v4/economic?name=retailMoneyFunds")
        ).rename(columns={"value": "retailMoneyFunds"})
        df = df.merge(
            pd.DataFrame(self.general_request("v4/economic?name=federalFunds")).rename(
                columns={"value": "federalFunds"}
            ),
            on="date",
            how="outer",
        )
        df = df.merge(
            pd.DataFrame(
                self.general_request(
                    "v4/economic?name=3MonthOr90DayRatesAndYieldsCertificatesOfDeposit"
                )
            ).rename(columns={"value": "3M_CD_rate"}),
            on="date",
            how="outer",
        )
        df = df.merge(
            pd.DataFrame(
                self.general_request(
                    "v4/economic?name=commercialBankInterestRateOnCreditCardPlansAllAccounts"
                )
            ).rename(columns={"value": "CC_interest_commercial"}),
            on="date",
            how="outer",
        )

        # convert to dataframe, parse date, raise exception if parsing can not be done
        try:
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
            df = df.set_index("date").tz_localize(None)  # remove time-zone information
        except ValueError:
            raise LookupError("Data table not consistent or of expected format")

        return df

    # Economic indicators
    def get_housing_indicators_US(self):
        """
        Weekly (interest rates) and annual (number of new buildings) economic
        indicators for the US. Contains the data listed below:

        # 15 or 30 years fixed rate mortgage average:
        The weekly mortgage rate is now based on applications submitted to
        Freddie Mac from lenders across the country. For more information
        regarding Freddie Mac’s enhancement, see their research note:
        https://www.freddiemac.com/research/insight/20221103-freddie-macs-newly-enhanced-mortgage-rate-survey

        # newPrivatelyOwnedHousingUnitsStartedTotalUnits:
        Thousands of Units, Seasonally Adjusted Annual Rate.
        New housing constructions.

        Returns
        -------
        df: (pd.DataFrame)
            Output data parsed to a pd.DataFrame object.
        """
        # initial checks and messages
        if not self.silent:
            print(f"Fetching US housing indicators")

        # get data
        df = pd.DataFrame(
            self.general_request("v4/economic?name=15YearFixedRateMortgageAverage")
        ).rename(columns={"value": "15Y_fixed_mortgage_rate"})
        df = df.merge(
            pd.DataFrame(
                self.general_request("v4/economic?name=30YearFixedRateMortgageAverage")
            ).rename(columns={"value": "30Y_fixed_mortgage_rate"}),
            on="date",
            how="outer",
        )
        df = df.merge(
            pd.DataFrame(
                self.general_request(
                    "v4/economic?name=newPrivatelyOwnedHousingUnitsStartedTotalUnits"
                )
            ).rename(columns={"value": "NewUnits_adjS"}),
            on="date",
            how="outer",
        )

        # convert to dataframe, parse date, raise exception if parsing can not be done
        try:
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
            df = df.set_index("date").tz_localize(None)  # remove time-zone information
        except ValueError:
            raise LookupError("Data table not consistent or of expected format")

        return df

    # ----------------------------------------------------------------------------- #
    # Miscellaneous
    # ----------------------------------------------------------------------------- #
    # Company upgrades/downgrades
    def get_upgrades_downgrades(self, symbol: Union[str, None] = None) -> pd.DataFrame:
        """
        Current company upgrades and downgrades consensus

        Parameters
        ----------
        symbol: (str or None)
            The symbol. If None, the default symbol will be used.

        Returns
        -------
        df: (pd.DataFrame)
            Output data parsed to a pd.DataFrame object.
        """
        # initial checks and messages
        symbol = self.helper_symbol(symbol=symbol)
        if not self.silent:
            print(f"Fetching upgrade/downgrade history for {symbol}")

        url = f"{self.api_address}/v4/upgrades-downgrades-consensus?symbol={symbol}&apikey={self.key}"
        data = self.get_jsonparsed_data(url)
        # convert to dataframe, raise exception if parsing can not be done
        try:
            df = pd.DataFrame(data)
        except ValueError:
            raise LookupError("Data table not consistent or of expected format")

        return df

    # Stocks: Insider trading
    def get_historical_insider_trade_symbol(
        self, symbol: Union[str, None] = None, num_pages=2
    ) -> pd.DataFrame:
        """
        Retrieve historical data for insider trading reports.

        Parameters
        ----------
        symbol: (str or None)
            The symbol. If None, the default symbol will be used.
        num_pages: (int)
            Number of pages of reports to call. 1 page = 1 API call

        Returns
        -------
        df: (pd.DataFrame)
            Output data parsed to a pd.DataFrame object.
        """
        # initial checks and messages
        symbol = self.helper_symbol(symbol=symbol)
        if not self.silent:
            print(f"Fetching {num_pages} pages of insider trading data for {symbol}.")
        # initialise a data frame and concatenate data for all pages
        df = pd.DataFrame()
        for page in range(num_pages):
            url = f"{self.api_address}/v4/insider-trading?symbol={symbol}&page={page}&apikey={self.key}"
            insider_trade = self.get_jsonparsed_data(url)
            df = pd.concat([df, pd.DataFrame(insider_trade)], axis=0)
        # convert to dataframe, parse date, raise exception if parsing can not be done
        try:
            df = df.reset_index(inplace=False)
            df = df.drop(columns=["index"])
            df["date"] = pd.to_datetime(df["transactionDate"], format="%Y-%m-%d")
            df = df.set_index("date").tz_localize(None)  # remove time-zone information
        except ValueError:
            raise LookupError("Data table not consistent or of expected format")
        return df
