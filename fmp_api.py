import pandas as pd
import json
import sys
from typing import Union
from urllib.request import urlopen
import requests


class FMP:
    def __init__(self,
                 symbol: Union[str, None] = None,
                 key_path: Union[str, None] = None,
                 silent: bool = False,
                 check_online: bool = False):
        """
        API class for Financial Modeling Prep that implements some
        endpoints available.
        The class can take a default symbol as an input, for which all
        methods will be evaluated, or an explicit symbol.
        General methods for the main data categories are available.

        The API key to FMP must be provided as a .json file and named
        keys.json to work.

        Parameters
        ----------
        symbol: (str or None)
            The symbol to be used as default for all evaluations.
            If None, it has to be explicitly inserted in each method.
        key_path: (str or None)
            The path to the API json file keys.json.
        silent: (bool)
            Should informative messages be printed?
        check_online: (bool)
            Check if the server is reachable
        """
        # Load API key
        if key_path is None:
            key_path = sys.path[-1]  # Should be root of the project, but check
        with open(str(key_path) + "/keys.json", "r") as key_file:
            keys = json.load(key_file)
        financial_modeling_prep = keys["financial_modeling_prep"]

        # setup
        self.symbol = symbol
        self.key = financial_modeling_prep
        self.silent = silent
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
    def get_jsonparsed_data(self, url):
        """
        Receive the content of "url", parse it as JSON and return the object.
        Checks if the returned data is empty or if an error occurred.

        Parameters
        ----------
        url: (str)
            url of the API

        Returns
        -------
        content: (json)
            parsed content returned from the API.
        """
        # get response from API
        response = urlopen(url)
        data = response.read().decode("utf-8")
        data = json.loads(data)

        # checks
        if len(data) == 0 or (isinstance(data, dict) and 'Error Message' in data.keys()):
            raise LookupError("Data table not found with FMP API.")

        return data

    def helper_symbol(self, symbol: Union[str, None] = None):
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
    def general_request(self,
                        message: str,
                        API_key_liaison: str = "&"):
        """
        General method for not-yet-implemented endpoints, API key inserted.
        Output parsed as .json.
        API liaison can be changed (necessary for some requests).
        """

        url = f"{self.api_address}/{message}{API_key_liaison}apikey={self.key}"

        return self.get_jsonparsed_data(url)

    # ----------------------------------------------------------------------------- #
    # Stock fundamentals
    # ----------------------------------------------------------------------------- #
    # Financial statements
    def get_income_statements(self,
                              symbol: Union[str, None] = None,
                              type: str = "auto"):
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

        # convert to data frame
        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
        df = df.set_index("date")

        return df

    def get_balance_sheet_statements(self,
                                     symbol: Union[str, None] = None,
                                     type: str = "auto"):
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

        # convert to data frame
        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
        df = df.set_index("date")

        return df

    def get_cash_flow_statements(self,
                                 symbol: Union[str, None] = None,
                                 type: str = "auto"):
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
        #
        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
        df = df.set_index("date")

        return df

    # Info
    def get_stock_info(self,
                       symbol: Union[str, None] = None):
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
            print(f"Fetching daily stock prices history for {symbol}")

        url = f"{self.api_address}/v3/profile/{symbol}?apikey={self.key}"

        return self.get_jsonparsed_data(url)[0]

    # Prices
    def get_prices_history_daily(self,
                                 symbol: Union[str, None] = None,
                                 date_start: Union[str, None] = "1900-01-01",
                                 date_end: Union[str, None] = None,
                                 type: str = "stock"):
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
        type: str
            "stock" (default), "currency" or "index"

        Returns
        -------
        df: (pd.DataFrame)
            Output data parsed to a pd.DataFrame object.
        """
        # initial checks and messages
        symbol = self.helper_symbol(symbol=symbol)
        if not self.silent:
            print(f"Fetching daily stock prices history for {symbol}")

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
        if type == "stock":
            url = f"{self.api_address}/v3/historical-price-full/{symbol}?{date_str}apikey={self.key}"
        elif type == "index":
            url = f"{self.api_address}/v3/historical-price-full/{symbol}?{date_str}apikey={self.key}"
        elif type == "currency":
            url = f"{self.api_address}/v3/historical-price-full/{symbol}?{date_str}apikey={self.key}"
        else:
            raise ValueError("Invalid input for 'type'")
        prices = self.get_jsonparsed_data(url)

        df = pd.DataFrame(prices["historical"])
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
        df = df.set_index("date")

        return df

    def get_stock_split_history(self, symbol: Union[str, None] = None):
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
            print("Fetching stock split history for {}".format(symbol))

        url = f"{self.api_address}/v3/historical-price-full/stock_split/{symbol}?&apikey={self.key}"
        stock_split = self.get_jsonparsed_data(url)

        df = pd.DataFrame(stock_split["historical"])
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
        df = df.set_index("date")

        return df

    # ----------------------------------------------------------------------------- #
    # Stock fundamental analyses
    # ----------------------------------------------------------------------------- #
    def get_financial_ratios(self,
                             symbol: Union[str, None] = None,
                             type: str = "auto"):
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
                url = f"{self.api_address}/v3/ratios/{symbol}?limit=120&apikey={self.key}"
                data = self.get_jsonparsed_data(url)
        elif type == "quarterly":
            url = f"{self.api_address}/v3/ratios/{symbol}?period=quarter?limit=400&apikey={self.key}"
            data = self.get_jsonparsed_data(url)
        elif type == "annually":
            url = f"{self.api_address}/v3/ratios/{symbol}?limit=120&apikey={self.key}"
            data = self.get_jsonparsed_data(url)
        else:
            raise NameError("Invalid input for type.")

        # convert to data frame
        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
        df = df.set_index("date")

        return df

    def get_enterprise_value(self,
                             symbol: Union[str, None] = None,
                             type: str = "quarterly"):
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

        # convert to data frame
        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
        df = df.set_index("date")

        return df

    def get_key_metrics(self,
                        symbol: Union[str, None] = None,
                        type: str = "quarterly"):
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

        # convert to data frame
        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
        df = df.set_index("date")

        return df

    def get_fmp_company_rating(self,
                               symbol: Union[str, None] = None):
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

        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
        df = df.set_index("date")

        return df

    def get_discounted_cashflow(self,
                                symbol: Union[str, None] = None):
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

        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
        df = df.set_index("date")

        return df

    # ----------------------------------------------------------------------------- #
    # Environmental, social and governance data
    # ----------------------------------------------------------------------------- #
    # ESG score
    def get_ESG_scores(self,
                       symbol: Union[str, None] = None):
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

        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
        df = df.set_index("date")

        return df

    # Company ESG risk rating
    def get_ESG_risk_rating(self,
                            symbol: Union[str, None] = None):
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

        url = f"{self.api_address}/v4/esg-environmental-social-governance-data-ratings" \
              f"?symbol={symbol}&apikey={self.key}"
        data = self.get_jsonparsed_data(url)

        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
        df = df.set_index("date")

        return df

    # ----------------------------------------------------------------------------- #
    # Stocks: Upgrades/downgrades
    # ----------------------------------------------------------------------------- #
    def get_upgrades_downgrades(self,
                                symbol: Union[str, None] = None):
        """
        Company upgrades and downgrades

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

        url = f"{self.api_address}/v4/upgrades-downgrades?symbol={symbol}&apikey={self.key}"
        data = self.get_jsonparsed_data(url)

        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["publishedDate"], format="%Y-%m-%d")
        df = df.set_index("date").tz_localize(None)  # remove time-zone information

        return df

    # ----------------------------------------------------------------------------- #
    # Stocks: Insider trading
    # ----------------------------------------------------------------------------- #
    def get_historical_insider_trade_symbol(self, symbol: Union[str, None] = None,
                                            num_pages=2):
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
            print("Fetching {} pages of {} insider trade data.".format(num_pages, symbol))

        df = pd.DataFrame()

        for page in range(num_pages):
            url = f"{self.api_address}/v4/insider-trading?symbol={symbol}&page={page}&apikey={self.key}"
            insider_trade = self.get_jsonparsed_data(url)
            df = pd.concat([df, pd.DataFrame(insider_trade)], axis=0)

        df = df.reset_index(inplace=False)
        df = df.drop(columns=["index"])
        df["date"] = pd.to_datetime(df["transactionDate"], format="%Y-%m-%d")
        df = df.set_index("date").tz_localize(None)  # remove time-zone information

        return df

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
        df = pd.DataFrame(self.general_request("v4/economic?name=retailMoneyFunds")). \
            rename(columns={"value": "retailMoneyFunds"})
        df = df.merge(pd.DataFrame(self.general_request("v4/economic?name=federalFunds")).
                      rename(columns={"value": "federalFunds"}), on="date", how="outer")
        df = df.merge(
            pd.DataFrame(self.general_request("v4/economic?name=3MonthOr90DayRatesAndYieldsCertificatesOfDeposit")).
            rename(columns={"value": "3M_CD_rate"}), on="date", how="outer")
        df = df.merge(pd.DataFrame(
            self.general_request("v4/economic?name=commercialBankInterestRateOnCreditCardPlansAllAccounts")).
                      rename(columns={"value": "CC_interest_commercial"}), on="date", how="outer")

        # convert date column and parse
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
        df = df.set_index("date").tz_localize(None)  # remove time-zone information

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
            print(f"Fetching US housing indicators...")

        # get data
        df = pd.DataFrame(self.general_request("v4/economic?name=15YearFixedRateMortgageAverage")). \
            rename(columns={"value": "15Y_fixed_mortgage_rate"})
        df = df.merge(pd.DataFrame(self.general_request("v4/economic?name=30YearFixedRateMortgageAverage")).
                      rename(columns={"value": "30Y_fixed_mortgage_rate"}), on="date", how="outer")
        df = df.merge(
            pd.DataFrame(self.general_request("v4/economic?name=newPrivatelyOwnedHousingUnitsStartedTotalUnits")).
            rename(columns={"value": "New_housing_units"}), on="date", how="outer")

        # convert date column and parse
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
        df = df.set_index("date").tz_localize(None)  # remove time-zone information

        return df

    # ----------------------------------------------------------------------------- #
    # Market indices and ETF symbols
    # ----------------------------------------------------------------------------- #
    def get_market_index_symbols(self,
                                 market_index: str):
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
            mi_dict = self.general_request("/v3/nasdaq_constituent", API_key_liaison="?")
        # Dow Jones
        elif market_index == "dj":
            mi_dict = self.general_request("/v3/dowjones_constituent", API_key_liaison="?")
        # EuroNext
        elif market_index == "euronext":
            mi_dict = self.general_request("/v3/symbol/available-euronext", API_key_liaison="?")
        # TSX
        elif market_index == "tsx":
            mi_dict = self.general_request("/v3/symbol/available-tsx", API_key_liaison="?")
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
