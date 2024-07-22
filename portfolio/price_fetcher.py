from abc import ABC, abstractmethod
from typing import List, Union, Optional
from datetime import date, timedelta, datetime
import pandas as pd
import yfinance as yf
import logging
from requests.exceptions import RequestException
from urllib3.exceptions import HTTPError
import importlib
import time
import os
from core.date_utils import parse_date

InputDate = Union[str, date, datetime]

class PriceFetcher(ABC):
    """
    Abstract base class for price fetchers.
    """
    @abstractmethod
    def fetch_prices(self, symbols: List[str], start_date: date, end_date: date) -> pd.DataFrame:
        """
        Fetch stock prices for the given symbols and date range.

        Args:
            symbols (List[str]): List of stock symbols to fetch prices for.
            start_date (date): Start date for the price data.
            end_date (date): End date for the price data.

        Returns:
            pd.DataFrame: A DataFrame containing the fetched price data.
        """
        pass

    @abstractmethod
    def fetch_current_price(self, symbol: str) -> Optional[float]:
        pass

class PriceFetcherPlugin(ABC):
    @abstractmethod
    def fetch_prices(self, symbols: List[str], start_date: InputDate, end_date: InputDate) -> pd.DataFrame:
        pass

class PriceFetcherManager:
    def __init__(self, plugin_dir: str):
        self.plugins = {}
        self._load_plugins(plugin_dir)

    def _load_plugins(self, plugin_dir: str):
        for filename in os.listdir(plugin_dir):
            if filename.endswith('.py') and not filename.startswith('_'):
                module_name = filename[:-3]
                module = importlib.import_module(f"plugins.{module_name}")
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if isinstance(attr, type) and issubclass(attr, PriceFetcher) and attr is not PriceFetcher:
                        self.plugins[module_name] = attr()

    def get_price_fetcher(self, name: str) -> PriceFetcher:
        if name in self.plugins:
            return self.plugins[name]
        elif name == 'yfinance':
            return YFinancePriceFetcher()
        else:
            raise ValueError(f"Unknown price fetcher: {name}")

class YFinancePriceFetcher(PriceFetcher):
    def __init__(self, max_retries=3, retry_delay=1, fallback_period=30):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.fallback_period = fallback_period

    def fetch_current_price(self, symbol: str) -> Optional[float]:
        try:
            ticker = yf.Ticker(symbol)
            current_price = ticker.info.get('regularMarketPrice')
            if current_price is not None:
                self.logger.info(f"Fetched current price for {symbol}: ${current_price:.2f}")
                return current_price
            else:
                self.logger.warning(f"Unable to fetch current price for {symbol}")
                return None
        except Exception as e:
            self.logger.error(f"Error fetching current price for {symbol}: {str(e)}")
            return None

    def fetch_prices(self, symbols: List[str], start_date: InputDate, end_date: InputDate) -> pd.DataFrame:
        start_date = parse_date(start_date)
        end_date = parse_date(end_date)
        self.logger.debug(f"Fetching prices for {symbols} from {start_date} to {end_date}")
        
        all_data = pd.DataFrame()
        
        for symbol in symbols:
            symbol_data = self._fetch_single_symbol(symbol, start_date, end_date)
            if not symbol_data.empty:
                all_data[symbol] = symbol_data

        if all_data.empty:
            self.logger.warning("No data fetched for any symbol")
        else:
            self.logger.debug(f"Fetched data:\n{all_data}")

        return all_data

    def _fetch_single_symbol(self, symbol: str, start_date: date, end_date: date) -> pd.Series:
        for attempt in range(self.max_retries):
            try:
                self.logger.debug(f"Attempting to fetch data for {symbol} (Attempt {attempt + 1})")
                data = yf.download(symbol, start=start_date, end=end_date + timedelta(days=1), progress=False)['Close']
                
                if data.empty:
                    self.logger.warning(f"yfinance returned empty DataFrame for {symbol} on attempt {attempt + 1}")
                    if attempt == self.max_retries - 1:  # Last attempt
                        return self._fetch_fallback(symbol, start_date, end_date)
                    time.sleep(self.retry_delay)
                    continue
                
                self.logger.debug(f"Successfully fetched data for {symbol}: {data}")
                return data

            except (RequestException, HTTPError) as e:
                self.logger.error(f"Network error for {symbol} on attempt {attempt + 1}: {str(e)}")
                if attempt == self.max_retries - 1:  # Last attempt
                    return self._fetch_fallback(symbol, start_date, end_date)
                time.sleep(self.retry_delay)
            except Exception as e:
                self.logger.error(f"Unexpected error for {symbol} on attempt {attempt + 1}: {str(e)}", exc_info=True)
                if attempt == self.max_retries - 1:  # Last attempt
                    return self._fetch_fallback(symbol, start_date, end_date)
                time.sleep(self.retry_delay)
        
        return pd.Series()

    def _fetch_fallback(self, symbol: str, start_date: date, end_date: date) -> pd.Series:
        self.logger.debug(f"Attempting fallback fetch for {symbol} with extended date range")
        extended_start = start_date - timedelta(days=self.fallback_period)
        extended_end = end_date + timedelta(days=self.fallback_period)
        
        try:
            data = yf.download(symbol, start=extended_start, end=extended_end + timedelta(days=1), progress=False)['Close']
            if not data.empty:
                self.logger.debug(f"Fallback fetch successful for {symbol}: {data}")
                return data[start_date:end_date]
            else:
                self.logger.warning(f"Fallback fetch for {symbol} returned empty DataFrame")
                return pd.Series()
        except Exception as e:
            self.logger.error(f"Fallback fetch for {symbol} failed: {str(e)}", exc_info=True)
            return pd.Series()
