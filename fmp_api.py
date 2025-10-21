import pandas as pd
import json
from typing import Union
from urllib.request import urlopen
import random
import os
import time
import datetime
from celery.result import AsyncResult
from urllib.error import URLError, HTTPError
from http.client import RemoteDisconnected, IncompleteRead
from modules.p00_task_queueing.celery_json.tasks import app as celery_app
from sqlpluspython.utils.paths import get_project_path
import sqlpluspython.utils.lists as lists
import sqlpluspython.utils.dictionaries as dicts
import sqlpluspython.db_connection as db
import modules.p00_task_queueing.celery_connection as cc
from sqlalchemy import Engine, text
from rich.progress import Progress

pd.set_option("future.no_silent_downcasting", True)

class RequestError(Exception):
    pass

class FMP:  # (metaclass=ExceptionHandlerMeta):
    def __init__(
        self,
        date_start: Union[str, None] = None,
        key_path: Union[str, None] = None,
        time_wait_retry: int = 10,
        time_wait_query: float = 0.5,
        retries: int = 3,
        task_queuing: str = "legacy",
        start_celery: bool = True,
        path_db_env: str = "modules/p00_databases/.env",
        fmp_free_account: bool = True,
        silent: bool = False,
    ):
        """
        API class for Financial Modelling Prep that implements some
        endpoints available.

        The class can take a default symbol as an input, for which all
        methods will be evaluated, or an explicit symbol.
        General methods for the main data categories are available.
        The backend can be legacy (i.e. urllib.request) or celery_json.

        The API key to FMP must be provided as a .json file and named
        keys.json to work. If no key_path is given, the current working
        directory will be used.

        Parameters
        ----------
        date_start: (str or None)
            The global start date as a lower cut-off for data
        key_path: (str or None)
            The path to the API JSON file keys.json
        silent: (bool)
            Should informative messages be printed?
        time_wait_retry: (int)
            Time to wait before a retry (in seconds)
        time_wait_query: (float)
            Time to wait before sending a query (in seconds)
        retries: (int)
            Number of times to try downloading again after a short break
        task_queuing: (str)
            Legacy (waiting and retrying), celery_wait or celery_submit
            (distributed task queuing)
        start_celery: (bool)
            If task_queuing is celery_json, should the workers be started?
        path_db_env: (str)
            Location of .env file used to create the Docker container for
            redis with. Container name and ports will be loaded from here.
        fmp_free_account: (bool)
            Restricted mode with only datapoints available for free accounts.
        silent: (bool)
            Should informative messages be printed?
        """
        # Check that the API key file exists
        if key_path is None:
            key_path = get_project_path("Investio")
        elif isinstance(key_path, str):
            pass
        else:
            raise TypeError("Invalid input for key_path. Must a string or None")
        # Load API key
        if os.path.isfile(os.path.normpath(os.path.join(key_path, "keys.json"))):
            with open(
                os.path.normpath(os.path.join(key_path, "keys.json")), "r"
            ) as key_file:
                keys = json.load(key_file)
            financial_modeling_prep = keys["financial_modeling_prep"]
        else:
            raise FileNotFoundError(f"Keys file not found at {key_path}")
        # assertions
        assert task_queuing in [
            "legacy",
            "celery_wait",
            "celery_submit",
        ], "task_queuing must be either 'legacy', 'celery_wait' or 'celery_submit'"
        assert isinstance(retries, int), "Invalid type for retries, must be an integer"
        assert isinstance(
            time_wait_retry, int
        ), "Invalid type for time_wait_retry, must be an integer"

        # initialise attributes
        if date_start is None:
            self.date_start = "2000-01-01"
        elif isinstance(date_start, str):
            assert len(date_start) == 10, "Date must be in the format YYYY-MM-DD"
            self.date_start = date_start
        else:
            raise ValueError("date_start must be either a string or None")
        self.key = financial_modeling_prep
        self.silent = silent
        self.task_queuing = task_queuing
        self.time_wait_retry = time_wait_retry
        self.time_wait_query = time_wait_query
        self.retries = retries
        # Celery attributes
        self.project_name = "Investio"
        self.celery_app = None
        self.celery_task0 = "json_request"
        self.celery_worker_concurrency = 24
        self.celery_backend = "redis"
        self.celery_debug = False
        self.flag_celery_workers_running = False
        # Data
        self.fmp_free_account = fmp_free_account
        self.api_address = "https://financialmodelingprep.com/api"
        self.symbol_col = "symbol"
        self.date_col = "date"
        self.current_year = datetime.date.today().year
        self.current_quarter = (datetime.date.today().month - 1) // 3
        self.series_symbols = {
            "Prices": "v3/historical-price-full",
            "Meta data": "v3/profile",
        }
        if not self.fmp_free_account:
            self.series_symbols.update(
                {
                    "Income": "v3/income-statement",
                    "Balance": "v3/balance-sheet-statement",
                    "Cash flow": "v3/cash-flow-statement",
                    "Financial ratios": "v3/ratios",
                    "Enterprise value": "v3/enterprise-values",
                    "FMP rating": "v3/historical-rating",
                    "Key metrics": "v3/key-metrics",
                }
            )
        self.number_of_sheets = len(self.series_symbols)
        self.provided_sheets = list(self.series_symbols.keys())
        self.mandatory_sheets = ["Prices", "Meta data"]
        self.key_ei_us = "FMP-EI-US"
        self.series_macro = {self.key_ei_us: "v4/economic", "Treasury": "v4/treasury"}
        self.start_celery = start_celery
        self.path_db_env = path_db_env
        self.worker_processes = None
        self.tasks = None

        # Instantiate the Celery app
        if self.task_queuing in [
            "celery_wait",
            "celery_submit",
        ]:
            self.celery_app = celery_app
            tsk = self.celery_app.tasks[self.celery_task0].__module__
            # optional celery_json workers
            if self.start_celery:
                self.celery_workers_start(
                    pool=None,
                    silent=silent,
                )
            self.flag_celery_workers_running = self.celery_workers_running()

    def __del__(self):
        """
        Method that will be called when the object is about to be destroyed.
        This is a good place to clean up resources like stopping workers.
        """
        print("FMP object is being destroyed, cleaning up resources...")

        # Make sure to check if the object has been fully initialised
        # as __del__ might be called if __init__ raises an exception
        if hasattr(self, "task_queuing") and self.task_queuing != "legacy":
            try:
                if hasattr(self, "worker_processes") and self.worker_processes:
                    print("Stopping all Celery workers...")
                    self.celery_workers_stop()
                    print("All workers stopped successfully.")
            except Exception as e:
                # Avoid raising exceptions in __del__ as they can be swallowed
                print(f"Error during cleanup: {e}")

    # ----------------------------------------------------------------------------- #
    # Helper methods
    # ----------------------------------------------------------------------------- #
    def celery_workers_start(
        self,
        pool: str = None,
        silent: bool = False,
    ):
        """
        Start Celery workers and app; pooling and concurrency settings can be controlled.
        """
        if self.task_queuing != "legacy":
            self.worker_processes = cc.celery_workers_start(
                concurrency=self.celery_worker_concurrency,
                pool=pool,
                worker_prefix="fmp",
                queue=self.celery_app.conf.task_default_queue,
                tasks_module=self.celery_app.tasks[self.celery_task0].__module__,
                path_dotenv_file=self.path_db_env,
                project_name=self.project_name,
                debug=self.celery_debug,
                silent=silent,
            )
        else:
            print("Task queuing is not using Celery, invocation ignored")

    def celery_workers_stop(self):
        """
        Stop all Celery workers.
        """
        if self.task_queuing != "legacy":
            cc.celery_workers_stop(worker_processes=self.worker_processes)
        else:
            print("Task queuing is not using Celery, invocation ignored")
        return None

    def celery_workers_running(self):
        """
        Check if the Celery workers are running
        """
        if self.task_queuing != "legacy":
            return cc.celery_workers_running(worker_processes=self.worker_processes)
        else:
            print("Task queuing is not using Celery, invocation ignored")
            return None

    def celery_submit_job(self, url, **kwargs):
        """
        Submit a job using the Celery framework.

        This method wraps the functionality of submitting a task to a Celery
        worker queue by invoking the 'submit_task' functionality from an external
        controller (cc). It utilises the "json_request" task type and passes the
        provided URL and keyword arguments to the task submission process.

        Args:
            url (str): The URL endpoint to which the JSON request task will be related.
            **kwargs: Arbitrary keyword arguments passed to the task during submission.

        Returns:
            None
        """
        return cc.submit_task(self.celery_task0, self.celery_app, url, **kwargs)

    def celery_submit_several_jobs(
        self,
        symbols: list,
        series: str,
        date_start: Union[str, None, bool] = None,
        freq_type: Union[str or None] = "auto",
    ) -> dict:
        """
        Submit jobs for several symbols to asynchronously download the
        data in the given series.
        """
        # 0: initialisation
        assert isinstance(symbols, list), "the given symbols must be a list"
        assert self.celery_workers_running(), "workers must be started first"

        # 1: submit tasks and create a dictionary with all asynchronous objects
        d = {}
        for symbol in symbols:
            d[symbol] = self.helper_data_auto_period(
                series=series, symbol=symbol, date_start=date_start, freq_type=freq_type
            )
        return d

    def celery_submit_several_jobs_all_data(
        self,
        symbols: list,
        date_start: Union[str, None] = None,
        freq_type: str = "auto",
        prices_only: bool = False,
        sheets: Union[None, list] = None,
        add_prices_metadata_sheets: bool = True,
        engine: Union[Engine, None] = None,
    ) -> dict:
        """
        Submit jobs for several symbols to asynchronously download all
        data series in self.series_symbols (modulo what is allowed by a
        free account if self.fmp_free_account is set to True).
        The frequency of the data series is set for all sheets
        except for 'Prices' and 'Meta data'.

        Date of existing data in a table can be auto-detected if the
        SQL engine and table information are provided.
        """
        # 0: initialisation
        print(f"Submitting tasks for {len(symbols)} symbols: ", end="")

        # 1: submit tasks and create a dictionary with the sheets as keys
        d = {}
        for symbol in symbols:
            d[symbol] = self.get_all_symbol_data(
                symbol=symbol,
                date_start=date_start,
                freq_type=freq_type,
                prices_only=prices_only,
                sheets=sheets,
                add_prices_metadata_sheets=add_prices_metadata_sheets,
                engine=engine,
            )
        # close connections (used to get latest data dates in database)
        if engine is not None:
            engine.dispose(close=True)

        # 2: rearranging nested dictionaries and return
        sheets = list(dicts.dict_first_val(d).keys())
        d = {key: {k: d[k][key] for k in d if key in d[k]} for key in sheets}
        print("Completed!")
        return d

    def celery_submit_several_jobs_all_macroeconomic_data(
        self,
        date_start: Union[str, None] = None,
    ) -> dict:
        """
        Economic indicators for the US for a variety of economic indicators,
        such as GDP, unemployment and inflation.
        The data is either weekly, monthly, quarterly or annual.
        """
        # 0: initialisation
        print(f"Submitting tasks for all macroeconomic data: ", end="")

        # 1: submit tasks and create a dictionary for all US macroeconomic data and return
        d = self.get_all_us_economic_indicators(date_start=date_start)
        print("Completed!")
        return d

    def process_celery_results(
        self,
        d: dict,
        mandatory_sheets: Union[bool, list, None],
        symbol_col: str = "symbol",
        drop_symbol_col: bool = True,
    ) -> dict:
        """
        Process the results stored in the redis backend to retrieve
        the downloaded data as dataframes and dictionaries.

        Symbols where data from the mandatory sheets are missing are
        automatically discarded (and the removed symbols recorded).
        """
        # 0: initialise
        print(" *** Processing downloaded data *** ")
        assert isinstance(d, dict), "the given data must be a dictionary"
        if isinstance(mandatory_sheets, bool) and mandatory_sheets:
            mandatory_sheets = self.mandatory_sheets
        elif mandatory_sheets is None:
            mandatory_sheets = []
        assert isinstance(
            mandatory_sheets, list
        ), "the mandatory_sheets input must be a list or None"

        # 1: process each sheet and symbol, combine data for special sheets
        # 1.1: process each sheet and symbol
        dp = {}
        d_progress = {}
        symbols_to_remove = []
        time_start = time.time()
        with Progress() as progress:
            # add tasks
            for sheet in d:
                d_progress[sheet] = progress.add_task(
                    f"[red]{sheet}", total=len(d[sheet])
                )
            for sheet in d:
                dp[sheet] = {}
                n = 0
                for symbol in d[sheet]:
                    dp[sheet][symbol] = cc.celery_process_results(x=d[sheet][symbol])
                    try:
                        if (
                            isinstance(dp[sheet][symbol], str)
                            and dp[sheet][symbol] == "FAILURE"
                        ):
                            raise RequestError("Celery task failed")
                        else:
                            dp[sheet][symbol] = self.helper_process_dict(
                                data=dp[sheet][symbol],
                                sheet=sheet,
                                symbol_col=symbol_col,
                                drop_symbol_col=drop_symbol_col,
                            )
                    except RequestError:
                        # catch failed tasks
                        if sheet in mandatory_sheets:
                            symbols_to_remove.append(symbol)
                            dp[sheet][symbol] = None
                        else:
                            dp[sheet][symbol] = None
                        n += 1
                        progress.update(d_progress[sheet], completed=n)
        time_end = time.time()
        # 1.2: optional combine of sheets if self.key_ei_us is in the processed sheets
        if self.key_ei_us in dp:
            df = pd.DataFrame()
            for name, df_tmp in dp[self.key_ei_us].items():
                df_tmp = df_tmp.rename(columns={"value": f"{self.key_ei_us}_{name}"})
                df = df.join(df_tmp, how="outer", on=None)
            dp[self.key_ei_us] = {"combined": df}
        print(f"Completed! (in {round((time_end - time_start) / 60, 2)}m)")

        # 2: post-processing
        # 2.1: remove symbols with missing mandatory sheets from all dicts
        symbols_to_remove = list(set(symbols_to_remove))
        for symbol in symbols_to_remove:
            for sheet in dp:
                del dp[sheet][symbol]
        print("Summary:")
        print(f"   Processed symbols: {n}")
        print(f"   Processed sheets: {len(d)}")
        print(f"   Removed symbols: {len(symbols_to_remove)}")
        print(f"   Remaining symbols: {n-len(symbols_to_remove)}")
        return dp

    def helper_upload(
        self,
        engine: Engine,
        data: Union[dict, pd.DataFrame],
        sheet: str,
        symbol: Union[str, None],
        table_name: str,
    ):
        """
        Helper method to upload a dataframe or dict, respectively,
        to the database
        """
        if sheet == "Meta data":
            db.upload_dict(
                engine=engine,
                symbol=symbol,
                d=data,
                table_name=table_name,
                symbol_col=self.symbol_col,
                dtype_map=None,
                alter_table=True,
                silent=True,
            )
        else:
            db.upload_df(
                engine=engine,
                symbol=symbol,
                df=data,
                table_name=table_name,
                categorical_cols=None,
                numeric_cols=None,
                date_col=self.date_col,
                symbol_col=self.symbol_col,
                drop_index_cols=False,
                set_index_date_col=True,
                set_index_symbol_col=True,
                update_latest=True,
                columns_to_drop=None,
                dtype_map=None,
                keep_keys_nans=False,
                raise_exception_keys_nans=True,
                raise_exception_overwrite_symbol_col=False,
                silent=True,
            )

    # %% Batch download
    def download_symbol_data(
        self,
        symbols: list,
        date_start: Union[str, None] = None,
        freq_type: str = "auto",
        sheets: Union[None, list] = None,
        add_prices_metadata_sheets: bool = True,
        stocks_database: str = "stocks_fmp",
    ) -> dict:
        """
        Download symbol data using FMP: sheets can be selected, along with the
        frequency of the additional symbol data (per default the highest frequency
        available is selected).

        The latest available date of existing data in a table will be
        auto-detected to avoid downloading unnecessary amounts of data.
        """
        # 0: initialisation
        # 0.1: check queuing method and other assertions
        assert (
            self.task_queuing == "celery_submit"
        ), "this method is only implemented for celery_submit task queuing"
        # 0.2: set mandatory sheets
        mandatory_sheets = ["Prices", "Meta data"]
        # 0.3: create the SQL engine
        engine = db.get_engine(database=stocks_database)
        # 0.4: submitting all downloading jobs
        print(" +++ Download all available symbol data (FMP) +++ ")
        if engine is not None:
            print("Reading newest data dates from the database")
        d = self.celery_submit_several_jobs_all_data(
            symbols=symbols,
            date_start=date_start,
            freq_type=freq_type,
            sheets=sheets,
            add_prices_metadata_sheets=add_prices_metadata_sheets,
            engine=engine,
        )

        # 1: monitor progress until all have been downloaded
        cc.celery_download_status(d=d)

        # 2: Processing results after downloads have been completed
        d = self.process_celery_results(d, mandatory_sheets=mandatory_sheets)

        return d

    def download_currency_data(
        self,
        base_currency: str = "USD",
        date_start: Union[str, None] = None,
        stocks_database: str = "stocks_fmp",
        currencies_database: str = "currencies_fmp",
    ):
        """
        Download data for all available currencies in the database

        The latest available date of existing data in a table will be
        auto-detected to avoid downloading unnecessary amounts of
        data.
        """
        # 0: initialisation
        # 0.1: check queuing method
        assert (
            self.task_queuing == "celery_submit"
        ), "this method is only implemented for celery_submit task queuing"
        # 0.2: set mandatory sheets
        mandatory_sheets = ["Prices"]
        # 0.3: create the SQL engine for stocks database to read
        engine_stocks = db.get_engine(database=stocks_database)
        # 0.4: create the SQL engine for currencies database to read
        engine_currencies = db.get_engine(database=currencies_database)

        # 1: get all different currencies in the 'stocks' database
        # 1.1: prepare the query to send
        query = "SELECT distinct `currency` as 'currency' FROM `Meta data`"
        # 1.2: retrieve currencies (raw)
        with engine_stocks.connect() as connection:
            # execute query
            result_proxy = connection.execute(text(query))
            # fetch all results
            results = result_proxy.fetchall()
            # parse results
            currencies = [row[0] for row in results]
        engine_stocks.dispose(close=True)
        # 1.3: post-treatment
        if None in currencies:
            currencies.remove(None)
        if base_currency in currencies:
            currencies.remove(base_currency)
        currencies = [c.upper() for c in currencies]
        symbols = [f"{c}{base_currency}=X" for c in currencies]

        # 2: submitting all downloading jobs
        print(" +++ Download all available currency data (FMP) +++ ")
        d = self.celery_submit_several_jobs_all_data(
            symbols=symbols,
            date_start=date_start,
            freq_type="auto",
            prices_only=True,
            engine=engine_currencies,
        )
        engine_currencies.dispose(close=True)

        # 3: monitor progress until all have been downloaded
        cc.celery_download_status(d=d)

        # 4: Processing results after downloads have been completed
        d = self.process_celery_results(d, mandatory_sheets=mandatory_sheets)
        return d

    def download_macroeconomic_data(
        self,
        date_start: Union[str, None] = None,
    ) -> dict:
        """
        Download all the macroeconomic data that is available in FMP.
        """
        # 0: initialisation
        # 0.1: check queuing method and other assertions
        d = dict()
        assert (
            self.task_queuing == "celery_submit"
        ), "this method is only implemented for celery_submit task queuing"

        # 1: submit jobs for downloading and create a dictionary with the table names
        print(" +++ Download all available symbol data (FMP) +++ ")
        # 1.1: US economic indicators
        d_macro = self.celery_submit_several_jobs_all_macroeconomic_data(
            date_start=date_start
        )
        d[self.get_all_us_economic_indicators(return_key=True)] = d_macro

        # 1: monitor progress until all have been downloaded
        cc.celery_download_status(d=d)

        # 2: Processing results after downloads have been completed
        d = self.process_celery_results(d, mandatory_sheets=None, drop_symbol_col=False)

        return d

    def update_db(self, d: dict, database: str, parallel: bool = False):
        """
        Update a database with the newest data.
        """
        # 0: initialise
        # 0.1: get env variables
        print(" *** Updating database *** ")
        db.load_env_variables(path=self.path_db_env)
        # 0.2: create the SQL engine
        engine = db.get_engine(database=database)

        # 1: upload
        d_progress = {}
        time_start = time.time()
        with Progress() as progress:
            # add progress bars
            for sheet in d:
                d_progress[sheet] = progress.add_task(
                    f"[red]{sheet}", total=len(d[sheet])
                )
            if parallel:
                raise NotImplementedError("Parallel mode is not yet implemented")
            else:
                for sheet in d:
                    n = 0
                    for symbol in d[sheet]:
                        # upload
                        self.helper_upload(
                            engine=engine,
                            data=d[sheet][symbol],
                            sheet=sheet,
                            symbol=symbol,
                            table_name=sheet,
                        )
                        # update progress bar
                        n += 1
                        progress.update(d_progress[sheet], completed=n)
                        engine.dispose(close=True)
        time_end = time.time()
        print(f"Completed! (in {round((time_end - time_start) / 60,2)}m)")

    # %% Other helper functions
    def helper_start_date(
        self,
        date_start: Union[str, None, bool] = None,
        engine: Union[Engine, None] = None,
        table_name: Union[None, str] = None,
        symbol: Union[None, str] = None,
        date_col: Union[None, str] = None,
        symbol_col: Union[None, str] = None,
        str_date: bool = False,
    ):
        """
        Returns self.start_date if date_start is not given, and if False is
        given, the same is returned.
        For all other inputs, an exception is raised.

        The standard date can also be automatically detected from a database
        if the SQL engine and symbol name are given.
        """
        # 0: initialisation
        assert (
            date_start is None
            or isinstance(date_start, str)
            or isinstance(date_start, bool)
        ), f"date_start must be None, a string or boolean: {date_start}"
        assert engine is None or isinstance(
            engine, Engine
        ), "engine must be None or a sqlalchemy engine"

        # 1: autodetect from SQL table or get from the other input variables
        if isinstance(engine, Engine):
            # 1A.0: assertions
            assert isinstance(table_name, str), "table_name must be a string"
            assert isinstance(symbol, str), "symbol must be a string"
            assert isinstance(date_col, str), "date_col must be a string"
            assert isinstance(symbol_col, str), "symbol_col must be a string"
            # 1A.1: get the latest date for the symbol and return
            out = db.get_latest_date_symbol(
                engine=engine,
                table_name=table_name,
                symbol=symbol,
                date_col=date_col,
                symbol_col=symbol_col,
                raise_exception=True,
            )
            if out is not None and str_date:
                return str(out.date())
            else:
                return out
        else:
            if date_start is None:
                return self.date_start
            elif isinstance(date_start, str):
                return date_start
            elif isinstance(date_start, bool) and not date_start:
                return date_start
            else:
                raise ValueError("date_start must be either a string, boolean or None")

    def get_json_parsed_data(self, url: str, **kwargs) -> json:
        """
        Receive the content of "url", parse it as JSON and return the object.
        Checks if the returned data is empty or if an error occurred.

        Parameters:
            url (str): The URL from which to retrieve and parse JSON data.
            **kwargs: Additional keyword arguments to pass to the Celery task

        Returns:
            dict: Parsed JSON data returned from the API endpoint.
        """
        # get a response from the API
        if self.task_queuing == "legacy":
            # try to download the data (a couple of times)
            n = 0
            response = None
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
                raise RequestError(f"Request error for the URL: {url}")
            # parse data
            data = response.read().decode("utf-8")
            data = json.loads(data)
            # checks
            if len(data) == 0 or (
                isinstance(data, dict) and "Error Message" in data.keys()
            ):
                raise RequestError("Data table not found with FMP API.")
            return data
        elif self.task_queuing == "celery_wait":
            assert self.flag_celery_workers_running, "Celery workers are not running"
            res = self.celery_submit_job(url, **kwargs)
            out = res.wait(propagate=True)
            res.forget()
            return out
        elif self.task_queuing == "celery_submit":
            assert (
                self.flag_celery_workers_running
            ), "celery_json workers are not running"
            return self.celery_submit_job(url, **kwargs)
        else:
            raise NotImplementedError("Invalid task queuing method")

    def helper_data_auto_period(
        self,
        series: str,
        symbol: Union[str, None],
        date_start: Union[str, None, bool] = None,
        freq_type: Union[str or None] = "auto",
        additional_string: str = "",
        date_liaison: str = "?",
    ):
        """
        Helper function to get data from the correct starting date, and
        with the highest frequency choosing from quarterly and annually,
        where applicable.
        An additional string can be provided before the API key.

        Returns None if the data is not available.
        """
        # 0: prepare parsing
        # 0.1: set start date
        date_start = self.helper_start_date(date_start=date_start)
        # 0.2: parse symbol string
        if isinstance(symbol, str):
            str_symbol = f"/{symbol}"
        elif symbol is None:
            str_symbol = ""
        else:
            raise ValueError("symbol must be either a string or None")

        # 1: format url and send request
        if freq_type is None:
            if not date_start:
                url = f"{self.api_address}/{series}{str_symbol}?{additional_string}apikey={self.key}"
            else:
                url = f"{self.api_address}/{series}{str_symbol}{date_liaison}from={date_start}{additional_string}&apikey={self.key}"
        elif freq_type == "auto":
            limit_annual = self.current_year - int(date_start[0:4])
            limit_quarter = 4 * limit_annual + self.current_quarter
            # try first with quarterly data (return if successful), otherwise format a string with an annual period
            try:
                url = f"{self.api_address}/{series}{str_symbol}?period=quarter&limit={limit_quarter}{additional_string}&apikey={self.key}"
                data = self.get_json_parsed_data(url)
                return data
            except RequestError:
                url = f"{self.api_address}/{series}{str_symbol}?period=annual&limit={limit_annual}{additional_string}&apikey={self.key}"
        elif freq_type == "quarterly":
            limit_quarter = (
                4 * (self.current_year - int(date_start[0:4])) + self.current_quarter
            )
            url = f"{self.api_address}/{series}{str_symbol}?period=quarter&limit={limit_quarter}{additional_string}&apikey={self.key}"
        elif freq_type == "annually":
            limit_annual = self.current_year - int(date_start[0:4])
            url = f"{self.api_address}/{series}{str_symbol}?period=annual&limit={limit_annual}&apikey={self.key}"
        else:
            raise ValueError("Invalid input for freq_type")

        # 2: send request and handle exceptions if no data is available
        try:
            data = self.get_json_parsed_data(url)
        except (RequestError, HTTPError):
            data = None
        return data

    def helper_process_dict(
        self,
        data: Union[dict, None],
        sheet: Union[str, None] = None,
        symbol_col: str = "symbol",
        drop_symbol_col: bool = True,
    ):
        """
        Process the dictionary returned from FMP to a dataframe (except
        when it is meta-data, in that case, a dictionary is returned),
        parse date, raise exception if the parsing cannot be done.

        If data is None (i.e. no data was downloaded), None is returned
        for consistency with methods in other objects.
        """
        if data is None or len(data) == 0:
            return None
        else:
            try:
                if sheet == "Prices":
                    out = pd.DataFrame(data["historical"])
                    out[self.date_col] = pd.to_datetime(
                        out[self.date_col], format="%Y-%m-%d"
                    )
                    out = out.set_index(self.date_col)
                elif sheet == "Meta data":
                    out = data[0]
                else:
                    out = pd.DataFrame(data)
                    out[self.date_col] = pd.to_datetime(
                        out[self.date_col], format="%Y-%m-%d"
                    )
                    out = out.set_index(self.date_col)
                    if drop_symbol_col:
                        out = out.drop(columns=symbol_col)
            except (
                RequestError,
                ValueError,
                AttributeError,
                TypeError,
                KeyError,
            ):
                if isinstance(data, dict) and "symbol" in data.keys():
                    str_exc = f"{data['symbol']}: "
                else:
                    str_exc = ""
                if sheet is None:
                    str_sheet = ""
                else:
                    str_sheet = f" for sheet {sheet}"
                raise RequestError(
                    f"{str_exc}Data missing or not consistent{str_sheet}"
                )
            return out

    def helper_return(self, data, sheet: Union[str, None]):
        """
        Helper to ensure that the right kind of object is returned depending
        on what the task queuing method is.
        """
        # A: convert to dataframe, parse date, raise exception if the parsing cannot be done
        if self.task_queuing == "legacy" or self.task_queuing == "celery_wait":
            return self.helper_process_dict(data=data, sheet=sheet)
        # B: with celery_json backend, return the async object directly
        else:
            return data

    # ----------------------------------------------------------------------------- #
    # General requests and collect all data
    # ----------------------------------------------------------------------------- #
    # Send a general request
    def general_request(self, message: str, api_key_liaison: str = "&") -> json:
        """
        General method for not-yet-implemented endpoints, API key inserted.
        Output parsed as .json.
        API liaison can be changed (necessary for some requests).
        """
        url = f"{self.api_address}/{message}{api_key_liaison}apikey={self.key}"
        return self.get_json_parsed_data(url)

    # Collect all data available for the symbol
    def get_all_symbol_data(
        self,
        symbol: str,
        date_start: Union[str, None] = None,
        freq_type: str = "auto",
        prices_only: bool = False,
        sheets: Union[None, list] = None,
        add_prices_metadata_sheets: bool = True,
        engine: Union[Engine, None] = None,
        raise_exceptions: bool = True,
    ) -> dict:
        """
        Collects all data for symbols.

        If a symbol does not have a certain type of data,
        the value of the corresponding key will be None.

        Parameters
        ----------
        symbol: (str)
            The symbol. If None, the default symbol will be used.
        date_start: str (optional)
            First date to fetch data from. If None, the default date, all available
            data will be used. Must be in format YYYY-MM-DD.
        freq_type: (str)
            "auto" (i.e. try quarterly, else annually), "quarterly" or "annually".
        prices_only: (bool)
            If True, only the price data will be collected, irrespective of what
            is given in the parameter 'sheets' and 'add_prices_metadata_sheets'.
        sheets: (list or None)
            If None, all implemented data will be downloaded. If a list, only the
            sheets in the list will be downloaded. Checks are performed.
        add_prices_metadata_sheets: (bool)
            If True, the Prices and Meta data sheets will always be added to the
            list of sheets to download
        engine: (Engine or None)
            SQL engine used to connect to the database to get information about
            the latest available data. If None, the given date_start will be used.
        raise_exceptions: (bool)
            Raises exceptions for missing data if True. If False, the dictionary of
            data will be None for the corresponding sheet.

        Returns
        -------
        dict
        """
        # 0: initialise
        # 0.1: initialise dictionary to store results in
        data = dict()
        # 0.2: prepare sheets to download
        # check that the inputs are meaningful
        if sheets is None:
            sheets = list(self.series_symbols.keys())
        elif isinstance(sheets, list):
            assert lists.is_sublist(
                sheets, list(self.series_symbols.keys())
            ), f"Not all sheets given are valid FMP data sheets: {sheets}"
        else:
            raise ValueError("sheets must be a list or None")
        # 0.3: add sheets that must be downloaded
        if add_prices_metadata_sheets:
            sheets = lists.union(sheets, ["Prices", "Meta data"])
        if prices_only:
            sheets = ["Prices"]

        # 1: download sheets
        for sheet in sheets:
            try:
                # 1.1: get (auto) start date (from a database, if engine is given)
                if not sheet == "Meta data":
                    date_start = self.helper_start_date(
                        date_start=date_start,
                        engine=engine,
                        table_name=sheet,
                        symbol=symbol,
                        date_col=self.date_col,
                        symbol_col=self.symbol_col,
                        str_date=True,
                    )
                # 1.2: Prices and metadata (usually the mandatory sheets)
                if sheet == "Prices":
                    data["Prices"] = self.get_prices_history_daily(
                        symbol=symbol, date_start=date_start, date_end=None
                    )
                elif sheet == "Meta data":
                    data["Meta data"] = self.get_stock_info(symbol=symbol)
                # 1.3: Additional data sheets
                elif sheet == "Income":
                    data["Income"] = self.get_income_statements(
                        symbol=symbol, date_start=date_start, freq_type=freq_type
                    )
                elif sheet == "Balance":
                    data["Balance"] = self.get_balance_sheet_statements(
                        symbol=symbol, date_start=date_start, freq_type=freq_type
                    )
                elif sheet == "Cash flow":
                    data["Cash flow"] = self.get_cash_flow_statements(
                        symbol=symbol, date_start=date_start, freq_type=freq_type
                    )
                elif sheet == "Financial ratios":
                    data["Financial ratios"] = self.get_financial_ratios(
                        symbol=symbol, date_start=date_start, freq_type=freq_type
                    )
                elif sheet == "Enterprise value":
                    data["Enterprise value"] = self.get_enterprise_value(
                        symbol=symbol, date_start=date_start, freq_type=freq_type
                    )
                elif sheet == "FMP rating":
                    data["FMP rating"] = self.get_fmp_company_rating(
                        symbol=symbol,
                        date_start=date_start,
                    )
                elif sheet == "Key metrics":
                    data["Key metrics"] = self.get_key_metrics(
                        symbol=symbol, date_start=date_start, freq_type=freq_type
                    )
                # 1.4: Growth additional data sheets
                elif sheet == "Cashflow growth":
                    data["Cashflow growth"] = self.get_fmp_cashflow_growth(
                        symbol=symbol, date_start=date_start, freq_type=freq_type
                    )
                elif sheet == "Income growth":
                    data["Income growth"] = self.get_fmp_income_growth(
                        symbol=symbol, date_start=date_start, freq_type=freq_type
                    )
                elif sheet == "Balance sheet growth":
                    data["Balance sheet growth"] = self.get_fmp_balance_sheet_growth(
                        symbol=symbol, date_start=date_start, freq_type=freq_type
                    )
                elif sheet == "Financial growth":
                    data["Financial growth"] = self.get_fmp_financial_growth(
                        symbol=symbol, date_start=date_start, freq_type=freq_type
                    )
                else:
                    raise ValueError(
                        f"The sheet '{sheet}' has not been implemented as a data source"
                    )
            except RequestError:
                if raise_exceptions:
                    raise
                else:
                    data[sheet] = None

        return data

    def get_all_us_economic_indicators(
        self,
        date_start: Union[str, None] = None,
        return_key: bool = False,
    ) -> Union[dict, str]:
        """
        Collects all US macroeconomic data

        Parameters
        ----------
        date_start: str (optional)
            First date to fetch data from. If None, the default date, all available
            data will be used. Must be in format YYYY-MM-DD.
        return_key: bool (optional)
            If True, only the designated key of the data series will be returned.

        Returns
        -------
        Union
            dict
                Contains dataframes or AsyncResult objects with the
                name of the data series
            str
                Key name
        """
        # 0: initialise
        # 0.1: define key of the data series
        key = self.key_ei_us
        if return_key:
            return key
        # 0.2: initialise dictionary to store results in
        data = dict()
        # 0.3: US economic indicators (relevant ones not in FRED)
        list_names = [
            # "GDP",
            # "realGDP",
            # "nominalPotentialGDP",
            # "realGDPPerCapita",
            "federalFunds",
            # "CPI",
            "inflationRate",
            "inflation",
            "retailSales",
            "consumerSentiment",
            "durableGoods",
            # "unemploymentRate",
            "totalNonfarmPayroll",
            "initialClaims",
            "industrialProductionTotalIndex",
            "newPrivatelyOwnedHousingUnitsStartedTotalUnits",
            "totalVehicleSales",
            "retailMoneyFunds",
            "smoothedUSRecessionProbabilities",
            "3MonthOr90DayRatesAndYieldsCertificatesOfDeposit",
            "commercialBankInterestRateOnCreditCardPlansAllAccounts",
            "30YearFixedRateMortgageAverage",
            "15YearFixedRateMortgageAverage",
        ]

        # 1: submit jobs to download indicators
        for name in list_names:
            data[name] = self.get_us_economic_indicators(
                name=name, date_start=date_start
            )
        return data

    # ----------------------------------------------------------------------------- #
    # Symbol fundamentals
    # ----------------------------------------------------------------------------- #
    # Financial statements
    def get_income_statements(
        self, symbol: str, date_start: Union[str, None] = None, freq_type: str = "auto"
    ):
        """
        Filed quarterly or annual income statements.

        Parameters
        ----------
        symbol: (str)
            The symbol. If None, the default symbol will be used
        date_start: str (optional)
            First date to fetch data from. If None, the default date, all available
            data will be used. Must be in format YYYY-MM-DD
        freq_type: (str)
            "auto" (try first quarterly, then annually if it fails), "quarterly"
            or "annually"

        Returns
        -------
        pd.DataFrame or None
            Output data parsed to a pd.DataFrame object.
        """
        # 0: initial checks and messages
        if self.fmp_free_account:
            raise ValueError(
                "The FMP free account does not allow access to income statement data"
            )
        else:
            if not (self.silent or self.task_queuing == "celery_submit"):
                print(f"Fetching income statements {freq_type} history for {symbol}")

            # 1: get data
            data = self.helper_data_auto_period(
                series=self.series_symbols["Income"],
                symbol=symbol,
                date_start=date_start,
                freq_type=freq_type,
            )

            # 2: prepare to return
            return self.helper_return(data=data, sheet=None)

    def get_balance_sheet_statements(
        self, symbol: str, date_start: Union[str, None] = None, freq_type: str = "auto"
    ):
        """
        Quarterly or annual balance sheet statements.

        Parameters
        ----------
        symbol: (str)
            The symbol. If None, the default symbol will be used
        date_start: str (optional)
            First date to fetch data from. If None, the default date, all available
            data will be used. Must be in format YYYY-MM-DD
        freq_type: (str)
            "auto" (try first quarterly, then annually if it fails), "quarterly"
            or "annually"

        Returns
        -------
        pd.DataFrame or None
            Output data parsed to a pd.DataFrame object.
        """
        # 0: initial checks and messages
        if self.fmp_free_account:
            raise ValueError(
                "The FMP free account does not allow access to balance sheet data"
            )
        else:
            if not (self.silent or self.task_queuing == "celery_submit"):
                print(
                    f"Fetching balance sheet statements {freq_type} history for {symbol}"
                )

            # 1: get data
            data = self.helper_data_auto_period(
                series=self.series_symbols["Balance"],
                symbol=symbol,
                date_start=date_start,
                freq_type=freq_type,
            )

            # 2: prepare to return
            return self.helper_return(data=data, sheet=None)

    def get_cash_flow_statements(
        self, symbol: str, date_start: Union[str, None] = None, freq_type: str = "auto"
    ):
        """
        Quarterly or annual cash flow statements.

        Parameters
        ----------
        symbol: (str)
            The symbol. If None, the default symbol will be used.
        date_start: str (optional)
            First date to fetch data from. If None, the default date, all available
            data will be used. Must be in format YYYY-MM-DD
        freq_type: (str)
            "auto" (try first quarterly, then annually if it fails), "quarterly"
            or "annually"

        Returns
        -------
        pd.DataFrame or None
            Output data parsed to a pd.DataFrame object.
        """
        # 0: initial checks and messages
        if self.fmp_free_account:
            raise ValueError(
                "The FMP free account does not allow access to cash flow data"
            )
        else:
            if not (self.silent or self.task_queuing == "celery_submit"):
                print(f"Fetching cash flow statements {freq_type} history for {symbol}")

            # 1: get data
            data = self.helper_data_auto_period(
                series=self.series_symbols["Cash flow"],
                symbol=symbol,
                date_start=date_start,
                freq_type=freq_type,
            )

            # 2: prepare to return
            return self.helper_return(data=data, sheet=None)

    # Info
    def get_stock_info(self, symbol: str):
        """
        Fetch symbol info and meta-data.

        Parameters
        ----------
        symbol: (str)
            The symbol.
            If None, the default symbol will be used

        Returns
        -------
        dict or None
            Output data parsed as a dict
        """
        # 0: initial checks and messages
        if not (self.silent or self.task_queuing == "celery_submit"):
            print(f"Fetching meta-data for {symbol}")

        # 1: send request
        data = self.helper_data_auto_period(
            series=self.series_symbols["Meta data"],
            symbol=symbol,
            date_start=False,
            freq_type=None,
            additional_string="",
        )

        # 2: process and return
        return self.helper_return(data=data, sheet="Meta data")

    # Prices
    def get_prices_history_daily(
        self,
        symbol: str,
        date_start: Union[str, None] = None,
        date_end: Union[str, None] = None,
    ):
        """
        Fetch daily symbol/index/currency prices of the symbol (open/high/low/
        close/adj. close), volume, change, etc.
        Per default data starting from "1900-01-01" (i.e. as much as is available
        essentially) is downloaded.

        Parameters
        ----------
        symbol: (str)
            The symbol. If None, the default symbol will be used.
        date_start: str (optional)
            First date to fetch data from. If None, the default date, all available
            data will be used. Must be in format YYYY-MM-DD.
        date_end: str
            Final date to fetch data to. If None, the default date, all available
            data will be used. Must be in format YYYY-MM-DD.

        Returns
        -------
        pd.DataFrame or None
            Output data parsed to a pd.DataFrame object.
        """
        # 0: initial checks and messages
        if not (self.silent or self.task_queuing == "celery_submit"):
            print(f"Fetching historical daily prices for {symbol}")

        # 1: prepare potential new string for end date
        # e.g. v3/historical-price-full/AAPL?from=2011-03-12&to=2019-03-12
        if date_end is None:
            date_str = ""
        else:
            date_str = f"&to={date_end}"

        # 2: send request
        data = self.helper_data_auto_period(
            series=self.series_symbols["Prices"],
            symbol=symbol,
            date_start=date_start,
            freq_type=None,
            additional_string=date_str,
        )

        # 3: prepare to return
        return self.helper_return(data=data, sheet="Prices")

    def get_stock_split_history(
        self,
        symbol: str,
        date_start: Union[str, None] = None,
        date_end: Union[str, None] = None,
    ):
        """
        Fetch symbol split history of the symbol.

        Parameters
        ----------
        symbol: (str)
            The symbol. If None, the default symbol will be used.
        date_start: str (optional)
            First date to fetch data from. If None, the default date, all available
            data will be used. Must be in format YYYY-MM-DD.
        date_end: str
            Final date to fetch data to. If None, the default date, all available
            data will be used. Must be in format YYYY-MM-DD.

        Returns
        -------
        pd.DataFrame or None
            Output data parsed to a pd.DataFrame object.
        """
        # 0: initial checks and messages
        if not (self.silent or self.task_queuing == "celery_submit"):
            print(f"Fetching symbol split history for {symbol}")

        # 1: prepare potential new string for end date
        if date_end is None:
            date_str = ""
        else:
            date_str = f"&to={date_end}"

        # 2: send request
        data = self.helper_data_auto_period(
            series="v3/historical-price-full/stock_split",
            symbol=symbol,
            date_start=date_start,
            freq_type=None,
            additional_string=date_str,
        )

        # 3: prepare to return
        return self.helper_return(data=data, sheet="Prices")

    # ----------------------------------------------------------------------------- #
    # Analyses
    # ----------------------------------------------------------------------------- #
    def get_financial_ratios(
        self, symbol: str, date_start: Union[str, None] = None, freq_type: str = "auto"
    ):
        """
        Quarterly or annual company financial ratios (annual if
        quarterly data are not available)

        Parameters
        ----------
        symbol: (str)
            The symbol. If None, the default symbol will be used.
        date_start: str (optional)
            First date to fetch data from. If None, the default date, all available
            data will be used. Must be in format YYYY-MM-DD.
        freq_type: (str)
            "auto" (try first quarterly, then annually if it fails), "quarterly"
            or "annually".

        Returns
        -------
        pd.DataFrame or None
            Output data parsed to a pd.DataFrame object.
        """
        # 0: initial checks and messages
        if self.fmp_free_account:
            raise ValueError(
                "The FMP free account does not allow access to financial ratios data"
            )
        else:
            if not (self.silent or self.task_queuing == "celery_submit"):
                print(f"Fetching financial ratios {freq_type} history for {symbol}")

            # 1: get data
            data = self.helper_data_auto_period(
                series=self.series_symbols["Financial ratios"],
                symbol=symbol,
                date_start=date_start,
                freq_type=freq_type,
            )

            # 2: prepare to return
            return self.helper_return(data=data, sheet=None)

    def get_fmp_cashflow_growth(
        self, symbol: str, date_start: Union[str, None] = None, freq_type: str = "auto"
    ):
        """
        The cash flow growth rate for a company.
        It measures how quickly a company's cash flow is growing:
        https://site.financialmodelingprep.com/developer/docs#cashflow-growth-statement-analysis

        Parameters
        ----------
        symbol: (str)
            The symbol.
            If None, the default symbol will be used
        date_start: str (optional)
            First date to fetch data from. If None, the default date, all available
            data will be used. Must be in format YYYY-MM-DD
        freq_type: (str)
            "auto" (try first quarterly, then annually if it fails),
            "quarterly" or "annually"

        Returns
        -------
        pd.DataFrame or None
            Output data parsed to a pd.DataFrame object.
        """
        # 0: initial checks and messages
        if not (self.silent or self.task_queuing == "celery_submit"):
            print(
                f"Fetching cash flow statement growth {freq_type} history for {symbol}"
            )

        # 1: get data
        data = self.helper_data_auto_period(
            series="v3/cash-flow-statement-growth",
            symbol=symbol,
            date_start=date_start,
            freq_type=freq_type,
        )

        # 2: prepare to return
        return self.helper_return(data=data, sheet=None)

    def get_fmp_income_growth(
        self, symbol: str, date_start: Union[str, None] = None, freq_type: str = "auto"
    ):
        """
        Income growth rate for a company.
        Measures how quickly a company's income is growing:
        https://site.financialmodelingprep.com/developer/docs#cashflow-growth-statement-analysis

        Parameters
        ----------
        symbol: (str)
            The symbol. If None, the default symbol will be used
        date_start: str (optional)
            First date to fetch data from. If None, the default date, all available
            data will be used. Must be in format YYYY-MM-DD
        freq_type: (str)
            "auto" (try first quarterly, then annually if it fails),
            "quarterly" or "annually"

        Returns
        -------
        pd.DataFrame or None
            Output data parsed to a pd.DataFrame object.
        """
        # 0: initial checks and messages
        if not (self.silent or self.task_queuing == "celery_submit"):
            print(f"Fetching income growth {freq_type} history for {symbol}")

        # 1: get data
        data = self.helper_data_auto_period(
            series="v3/income-statement-growth",
            symbol=symbol,
            date_start=date_start,
            freq_type=freq_type,
        )

        # 2: prepare to return
        return self.helper_return(data=data, sheet=None)

    def get_fmp_balance_sheet_growth(
        self, symbol: str, date_start: Union[str, None] = None, freq_type: str = "auto"
    ):
        """
        Balance sheet growth rate for a company.
        It measures how quickly a company's balance sheet is growing:
        https://site.financialmodelingprep.com/developer/docs#balance-sheet-growth-statement-analysis

        Parameters
        ----------
        symbol: (str)
            The symbol. If None, the default symbol will be used
        date_start: str (optional)
            First date to fetch data from. If None, the default date, all available
            data will be used. Must be in format YYYY-MM-DD
        freq_type: (str)
            "auto" (try first quarterly, then annually if it fails),
            "quarterly" or "annually"

        Returns
        -------
        pd.DataFrame or None
            Output data parsed to a pd.DataFrame object.
        """
        # 0: initial checks and messages
        if not (self.silent or self.task_queuing == "celery_submit"):
            print(f"Fetching balance sheet growth {freq_type} history for {symbol}")

        # 1: get data
        data = self.helper_data_auto_period(
            series="v3/balance-sheet-statement-growth",
            symbol=symbol,
            date_start=date_start,
            freq_type=freq_type,
        )

        # 2: prepare to return
        return self.helper_return(data=data, sheet=None)

    def get_fmp_financial_growth(
        self, symbol: str, date_start: Union[str, None] = None, freq_type: str = "auto"
    ):
        """
        Financial growth rate for a company.
        Measure how quickly a company's financials are growing:
        https://site.financialmodelingprep.com/developer/docs#balance-sheet-growth-statement-analysis

        Parameters
        ----------
        symbol: (str)
            The symbol. If None, the default symbol will be used
        date_start: str (optional)
            First date to fetch data from. If None, the default date, all available
            data will be used. Must be in format YYYY-MM-DD
        freq_type: (str)
            "auto" (try first quarterly, then annually if it fails),
            "quarterly" or "annually"

        Returns
        -------
        pd.DataFrame or None
            Output data parsed to a pd.DataFrame object.
        """
        # 0: initial checks and messages
        if not (self.silent or self.task_queuing == "celery_submit"):
            print(f"Fetching financial growth {freq_type} history for {symbol}")

        # 1: get data
        data = self.helper_data_auto_period(
            series="v3/financial-growth",
            symbol=symbol,
            date_start=date_start,
            freq_type=freq_type,
        )

        # 2: prepare to return
        return self.helper_return(data=data, sheet=None)

    def get_enterprise_value(
        self,
        symbol: str,
        date_start: Union[str, None] = None,
        freq_type: str = "quarterly",
    ):
        """
        Quarterly or annual company enterprise value (annual if the
        quarterly are not available).

        Parameters
        ----------
        symbol: (str)
            The symbol: If None, the default symbol will be used
        date_start: str (optional)
            First date to fetch data from. If None, the default date, all available
            data will be used. Must be in format YYYY-MM-DD
        freq_type: (str)
            "auto" (try first quarterly, then annually if it fails),
            "quarterly" or "annually"

        Returns
        -------
        pd.DataFrame or None
            Output data parsed to a pd.DataFrame object.
        """
        # 0: initial checks and messages
        if self.fmp_free_account:
            raise ValueError(
                "The FMP free account does not allow access to enterprise value data"
            )
        else:
            if not (self.silent or self.task_queuing == "celery_submit"):
                print(f"Fetching enterprise values {freq_type} history for {symbol}")

            # 1: get data
            data = self.helper_data_auto_period(
                series=self.series_symbols["Enterprise value"],
                symbol=symbol,
                date_start=date_start,
                freq_type=freq_type,
            )

            # 2: prepare to return
            return self.helper_return(data=data, sheet=None)

    def get_key_metrics(
        self,
        symbol: str,
        date_start: Union[str, None] = None,
        freq_type: str = "quarterly",
    ):
        """
        Quarterly or annual company key metrics (annual if the
        quarterly data are not available).

        Parameters
        ----------
        symbol: (str)
            The symbol. If None, the default symbol will be used
        date_start: str (optional)
            First date to fetch data from. If None, the default date, all available
            data will be used. Must be in format YYYY-MM-DD
        freq_type: (str)
            "auto" (i.e. try quarterly, else annually), "quarterly"
            or "annually"

        Returns
        -------
        pd.DataFrame or None
            Output data parsed to a pd.DataFrame object.
        """
        # 0: initial checks and messages
        if self.fmp_free_account:
            raise ValueError(
                "The FMP free account does not allow access to key metrics data"
            )
        else:
            if not (self.silent or self.task_queuing == "celery_submit"):
                print(f"Fetching key metrics {freq_type} history for {symbol}")

            # 1: get data
            data = self.helper_data_auto_period(
                series=self.series_symbols["Key metrics"],
                symbol=symbol,
                date_start=date_start,
                freq_type=freq_type,
            )

            # 2: prepare to return
            return self.helper_return(data=data, sheet=None)

    def get_fmp_company_rating(self, symbol: str, date_start: Union[str, None] = None):
        """
        Daily company rating from FPM using their method:
        https://site.financialmodelingprep.com/developer/docs/recommendations-formula/

        TODO
            better estimate of limit to avoid downloading too much

        Parameters
        ----------
        symbol: (str)
            The symbol. If None, the default symbol will be used
        date_start: str (optional)
            First date to fetch data from. If None, the default date, all available
            data will be used. Must be in format YYYY-MM-DD

        Returns
        -------
        pd.DataFrame or None
            Output data parsed to a pd.DataFrame object.
        """
        # 0: initial checks and messages
        if self.fmp_free_account:
            raise ValueError(
                "The FMP free account does not allow access to FMP company data"
            )
        else:
            if not (self.silent or self.task_queuing == "celery_submit"):
                print(f"Fetching FMP company rating history for {symbol}")

            # 1: prepare and fetch data
            # 1.1 get estimate of number of days ('limit' must be used for this endpoint instead of 'from')
            date_start = self.helper_start_date(date_start=date_start)
            limit_Bdays_estimate = (self.current_year - int(date_start[0:4])) * 261
            # 1.2: send request
            data = self.helper_data_auto_period(
                series=self.series_symbols["FMP rating"],
                symbol=symbol,
                date_start=date_start,
                freq_type=None,
                additional_string=f"&limit={limit_Bdays_estimate}",
            )

            # 2: prepare to return
            return self.helper_return(data=data, sheet=None)

    # Environmental, social and governance (ESG) scores
    def get_esg_scores(self, symbol: str):
        """
        ESG score (mostly quarterly)

        TODO needs updating of endpoint

        Parameters
        ----------
        symbol: (str)
            The symbol. If None, the default symbol will be used.

        Returns
        -------
        pd.DataFrame or None
            Output data parsed to a pd.DataFrame object.
        """
        # 0: initial checks and messages
        if not (self.silent or self.task_queuing == "celery_submit"):
            print(f"Fetching ESG score history for {symbol}")

        # 1: fetch data
        data = self.helper_data_auto_period(
            series="v4/esg-environmental-social-governance-data",
            symbol=symbol,
            date_start=False,
            freq_type=None,
            additional_string="",
        )

        # 2: prepare to return
        return self.helper_return(data=data, sheet=None)

    # Company ESG risk rating
    def get_esg_risk_rating(self, symbol: str):
        """
        Annual ESG risk rating

        TODO needs updating of endpoint

        Parameters
        ----------
        symbol: (str)
            The symbol. If None, the default symbol will be used.

        Returns
        -------
        pd.DataFrame or None
            Output data parsed to a pd.DataFrame object.
        """
        # 0: initial checks and messages
        if not (self.silent or self.task_queuing == "celery_submit"):
            print(f"Fetching ESG risk rating history for {symbol}")

        # 1: fetch data
        data = self.helper_data_auto_period(
            series="v4/esg-environmental-social-governance-data-ratings",
            symbol=symbol,
            date_start=False,
            freq_type=None,
            additional_string="",
        )

        # 2: prepare to return
        return self.helper_return(data=data, sheet=None)

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
                - sp500: Standard and Poor's 500, tracking the symbol performance of 500 large companies
                         listed on symbol exchanges in the United States.
                - nasdaq100: The Nasdaq-100 (^NDX[2]) is a symbol market index made up of 101 equity
                             securities issued by 100 of the largest non-financial companies listed
                             on the Nasdaq symbol exchange.
                - dj: The Dow Jones Industrial Average (DJIA), Dow Jones or simply the Dow, is a
                      symbol market index of 30 prominent companies listed on symbol exchanges in the
                      United States.
                - euronext: Euronext N.V. (short for European New Exchange Technology) is a pan-European
                            bourse that offers various trading and post-trade services.
                - tsx: Toronto Stock Exchange is a symbol exchange located in Toronto, Ontario, Canada.
                - ETFs: List of ETF symbols currently traded

        Returns
        -------
        list:
            list of symbols in the market index.
        """
        # 1: get dictionary of market indices
        # S&P500
        if market_index == "sp500":
            mi_dict = self.general_request("/v3/sp500_constituent", api_key_liaison="?")
        # Nasdaq100
        elif market_index == "nasdaq100":
            mi_dict = self.general_request(
                "/v3/nasdaq_constituent", api_key_liaison="?"
            )
        # Dow Jones
        elif market_index == "dj":
            mi_dict = self.general_request(
                "/v3/dowjones_constituent", api_key_liaison="?"
            )
        # EuroNext
        elif market_index == "euronext":
            mi_dict = self.general_request(
                "/v3/symbol/available-euronext", api_key_liaison="?"
            )
        # TSX
        elif market_index == "tsx":
            mi_dict = self.general_request(
                "/v3/symbol/available-tsx", api_key_liaison="?"
            )
        # ETFs
        elif market_index == "ETFs":
            mi_dict = self.general_request("/v3/etf/list", api_key_liaison="?")
        else:
            raise ValueError("Invalid market_index")

        # wait for the result to be finished if celery_submit
        if self.task_queuing == "celery_submit":
            out = mi_dict.wait(propagate=True)
            mi_dict.forget()
            mi_dict = out

        # 2: create a list of symbols
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
            url_str = "stable/search-symbol"
            if reset:
                random.seed(seed)
            # get symbols
            list_symbols = self.general_request(message=url_str, api_key_liaison="?")
            # wait for the result to be finished if celery_submit
            if self.task_queuing == "celery_submit":
                out = list_symbols.wait(propagate=True)
                list_symbols.forget()
                list_symbols = out
            # select the number of symbols requested
            if n is None:
                symbols = list_symbols
            elif isinstance(n, int):
                symbols = random.sample(list_symbols, k=n)
            else:
                raise ValueError("Invalid input for n")
        # case B: Select stocks satisfying certain criteria
        elif isinstance(search_parameters, dict):
            # initialise (number of stocks also put in here)
            url_str = f"v3/symbol-screener?limit={n}"
            # prepare a list with parsed criteria
            for key in search_parameters:
                url_str = f"{url_str}&{key}={search_parameters[key]}"
            # get meta data (list of dicts)
            list_metadata = self.general_request(message=url_str, api_key_liaison="&")
            # wait for the result to be finished if celery_submit
            if self.task_queuing == "celery_submit":
                out = list_metadata.wait(propagate=True)
                list_metadata.forget()
                list_metadata = out
            # select symbol symbols
            symbols = []
            for d in list_metadata:
                symbols.append(d["symbol"])
        else:
            raise ValueError("Invalid input for search_parameters")

        return symbols

    def get_random_symbol_with_financial_statements(
        self, n=1, reset: bool = False, seed: int = 45
    ):
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

        # step 2: get a list of symbols with financial statements on FMP
        list_symbols = self.general_request(
            message="v3/financial-statement-symbol-lists", api_key_liaison="?"
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
    # US economic indicators
    def get_us_economic_indicators(
        self, name: str, date_start: Union[str, None] = None, return_key: bool = False
    ) -> Union[pd.DataFrame, AsyncResult, str]:
        """
        Economic indicators for the US for a variety of economic indicators,
        such as GDP, unemployment and inflation.
        The data is either weekly, monthly, quarterly or annual.

        Parameters
        ----------
        name: str
            The name of the data series, can be:
            GDP, realGDP, nominalPotentialGDP, realGDPPerCapita, federalFunds, CPI,
            inflationRate, inflation, retailSales, consumerSentiment, durableGoods,
            unemploymentRate, totalNonfarmPayroll, initialClaims,
            industrialProductionTotalIndex, newPrivatelyOwnedHousingUnitsStartedTotalUnits,
            totalVehicleSales, retailMoneyFunds, smoothedUSRecessionProbabilities,
            3MonthOr90DayRatesAndYieldsCertificatesOfDeposit,
            commercialBankInterestRateOnCreditCardPlansAllAccounts,
            30YearFixedRateMortgageAverage, 15YearFixedRateMortgageAverage
        date_start: str (optional)
            First date to fetch data from.
            If None, the default date, all available
            data will be used.
            Must be in format YYYY-MM-DD.
        return_key: bool (optional)
            If True, only the designated key of the data series will be returned.

        Returns
        -------
        Union
            str
                Designated key of the data series is returned if return_key
            AsyncResult
                Otherwise, if not return_key and the task queuing is celery_submit,
                an AsyncResult object is returned
            pd.DataFrame
                Otherwise if the task queuing is not celery_submit,
                the output data parsed as a pd.DataFrame object
        """
        # 0: Initialisation
        if self.fmp_free_account:
            raise ValueError(
                "The FMP free account does not allow access to enterprise value data"
            )
        else:
            # 0.1: define key of the data series
            key = self.key_ei_us
            if return_key:
                return key
            # 0.2: initial checks and messages
            if not (self.silent or self.task_queuing == "celery_submit"):
                print(f"Fetching the economic indicator {name}")

            # 1: get data
            data = self.helper_data_auto_period(
                series=f"{self.series_macro[key]}?name={name}",
                symbol=None,
                date_start=date_start,
                freq_type=None,
                date_liaison="&",
            )

            # 2: prepare to return
            # 2A: convert to dataframe, parse date, raise exception if the parsing cannot be done
            if self.task_queuing == "legacy" or self.task_queuing == "celery_wait":
                df = self.helper_process_dict(
                    data=data, sheet=None, drop_symbol_col=False
                )
                return df.rename(columns={"value": f"{key}_{name}"})
            # 2B: with celery_json backend, return the async object directly
            else:
                return data

    def get_us_treasury_rates(
        self, date_start: Union[str, None] = None, return_key: bool = False
    ) -> Union[pd.DataFrame, AsyncResult, str]:
        """
        US Treasury yield rates.

        Parameters
        ----------
        date_start: str (optional)
            First date to fetch data from.
            If None, the default date, all available
            data will be used.
            Must be in format YYYY-MM-DD.
        return_key: bool (optional)
            If True, only the designated key of the data series will be returned.

        Returns
        -------
        Union
            str
                Designated key of the data series is returned if return_key
            AsyncResult
                Otherwise, if not return_key and the task queuing is celery_submit,
                an AsyncResult object is returned
            pd.DataFrame
                Otherwise if the task queuing is not celery_submit,
                the output data parsed as a pd.DataFrame object
        """
        # 0: Initialisation
        # 0.1: define key of the data series
        key = "Treasury"
        if return_key:
            return key
        # 0.2: initial checks and messages
        if not (self.silent or self.task_queuing == "celery_submit"):
            print(f"Fetching the US treasury rates")

        # 1: get data
        data = self.helper_data_auto_period(
            series=self.series_macro[key],
            symbol=None,
            date_start=date_start,
            freq_type=None,
            date_liaison="?",
        )

        # 2: prepare to return
        # 2A: convert to dataframe, parse date, raise exception if the parsing cannot be done
        if self.task_queuing == "legacy" or self.task_queuing == "celery_wait":
            df = self.helper_process_dict(data=data, sheet=None, drop_symbol_col=False)
            return df
        # 2B: with celery_json backend, return the async object directly
        else:
            return data


# %%
if __name__ == "__main__2":
    # Base object from FMP class
    fmp = FMP(silent=False, task_queuing="legacy")
    # fmp = FMP(silent=False, task_queuing="celery_wait", start_celery=True)
    # fmp = FMP(silent=False, task_queuing="celery_submit", start_celery=True)
    fmp.celery_app_initialise()
    fmp.celery_workers_start()
    fmp.celery_workers_running()
    fmp.celery_workers_stop()
    # ----------------------------------------------------------------------------- #
    # Basics
    # ----------------------------------------------------------------------------- #
    # general request
    req1 = fmp.general_request(
        "/v3/historical-price-full/MSFT?from=2000-03-12"
    )  # working
    fmp.general_request(
        "/v4/financial-reports-json?symbol=ALO.PA&year=2020&period=Q1"
    )  # not working example
    # all data available for the symbol
    fmp.date_start = "2010-06-01"
    dict_data1 = fmp.get_all_symbol_data(symbol="MSFT", sheets=["FMP ratings"])
    dict_data2 = fmp.get_all_symbol_data(symbol="MSFT", freq_type="auto")
    dict_data3 = fmp.get_all_symbol_data(symbol="1248.HK", freq_type="annually")

    # ----------------------------------------------------------------------------- #
    # Fundamentals
    # ----------------------------------------------------------------------------- #
    dict_info = fmp.get_stock_info(symbol="AAPL")
    df1 = fmp.get_prices_history_daily(symbol="AAPL")  # all available data
    df2 = fmp.get_prices_history_daily(symbol="^TYX")
    df3 = fmp.get_prices_history_daily(symbol="USDDKK")
    df5 = fmp.get_income_statements(symbol="MSFT")
    df6 = fmp.get_balance_sheet_statements("MSFT")
    df7 = fmp.get_cash_flow_statements("MSFT", "auto")
    df8 = fmp.get_stock_split_history(symbol="MSFT")

    # ----------------------------------------------------------------------------- #
    # Analyses
    # ----------------------------------------------------------------------------- #
    df9 = fmp.get_financial_ratios(symbol="MSFT", freq_type="quarterly")
    df10 = fmp.get_financial_ratios(symbol="MSFT", freq_type="auto")
    df11 = fmp.get_fmp_cashflow_growth(symbol="MSFT", freq_type="auto")
    df12 = fmp.get_fmp_income_growth(symbol="MSFT")
    df13 = fmp.get_fmp_balance_sheet_growth(symbol="MSFT")
    df14 = fmp.get_fmp_financial_growth(symbol="MSFT")
    df15 = fmp.get_enterprise_value(symbol="MSFT")
    df16 = fmp.get_key_metrics(symbol="MSFT")
    df17 = fmp.get_fmp_company_rating(symbol="MSFT")
    df19 = fmp.get_esg_scores(symbol="MSFT")
    df20 = fmp.get_esg_risk_rating(symbol="MSFT")

    # ----------------------------------------------------------------------------- #
    # Random symbols, market indices and ETF symbols:
    # ----------------------------------------------------------------------------- #
    symbols1 = fmp.get_market_index_symbols("nasdaq100")
    symbols2 = fmp.get_symbol()
    symbols3 = fmp.get_symbol(search_parameters={"sector": "Technology"}, n=10)
    symbols4 = fmp.get_random_symbol_with_financial_statements()
    dict_sectors = fmp.get_symbols_per_sector(return_counts=False)
    df21 = fmp.get_symbols_per_sector(return_counts=True)

    # ----------------------------------------------------------------------------- #
    # Macroeconomic data
    # ----------------------------------------------------------------------------- #
    # US economic indicators
    df22 = fmp.get_all_us_economic_indicators()
    df23 = fmp.get_us_economic_indicators(name="nominalPotentialGDP")
    df24 = fmp.get_us_economic_indicators(name="30YearFixedRateMortgageAverage")
    df25 = fmp.get_us_economic_indicators(name="consumerSentiment")
    # download all macroeconomic data
    d_ = fmp.download_macroeconomic_data(date_start=None)
    # US treasury rates
    df26 = fmp.get_us_treasury_rates(date_start="2020-01-01")
