from __future__ import annotations

import sqlite3
from typing import Dict, List, Optional, Set
from datetime import datetime, date, timedelta
import pandas as pd
import logging
from .price_fetcher import PriceFetcher, YFinancePriceFetcher, PriceFetcherManager
from .transaction import Transaction, CashFlow
from decimal import Decimal
from core.config import DEFAULT_DB_PATH, LOG_FILE_PATH
from core.date_utils import parse_date, InputDate
import traceback
import threading

pd.set_option('future.no_silent_downcasting', True)

class PortfolioValueError(Exception):
    pass

class PriceDataError(Exception):
    pass

class PortfolioError(Exception):
    pass

def setup_logger(name, log_file, level=logging.INFO):
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    
    return logger

class ThreadSafeDatabaseManager:
    def __init__(self, db_file: str):
        self.db_file = db_file
        self._local = threading.local()

    @property
    def conn(self):
        if not hasattr(self._local, 'conn'):
            self._local.conn = sqlite3.connect(self.db_file, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
            self._local.conn.row_factory = sqlite3.Row
        return self._local.conn

    def __enter__(self):
        return self.conn.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.conn.commit()
        else:
            self.conn.rollback()

    def close(self):
        if hasattr(self._local, 'conn'):
            self._local.conn.close()
            del self._local.conn

class DatabaseManager:
    def __init__(self, db_file: str):
        self.db_file = db_file

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_file, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        self.conn.row_factory = sqlite3.Row
        self.init_db()
        return self.conn.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.conn.commit()
        else:
            self.conn.rollback()
        self.conn.close()

    def init_db(self):
        statements = [
            '''
            CREATE TABLE IF NOT EXISTS price_cache (
                symbol TEXT,
                date DATE,
                price DECIMAL(15,2),
                PRIMARY KEY (symbol, date)
            )
            ''',
            'CREATE INDEX IF NOT EXISTS idx_price_cache_date ON price_cache(date)',
            'PRAGMA table_info(price_cache)',
            '''
            CREATE TABLE IF NOT EXISTS price_cache_new (
                symbol TEXT,
                date DATE,
                price DECIMAL(15,2),
                PRIMARY KEY (symbol, date)
            )
            ''',
            'CREATE INDEX IF NOT EXISTS idx_price_cache_new_date ON price_cache_new(date)',
            'INSERT OR REPLACE INTO price_cache_new SELECT * FROM price_cache',
            'DROP TABLE IF EXISTS price_cache',
            'ALTER TABLE price_cache_new RENAME TO price_cache',
            'PRAGMA table_info(price_cache)',
            '''
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY,
                date DATE NOT NULL,
                symbol TEXT NOT NULL,
                action TEXT NOT NULL,
                amount DECIMAL(15,2) NOT NULL,
                price DECIMAL(15,2) NOT NULL,
                fees DECIMAL(15,2) NOT NULL
            )
            ''',
            '''
            CREATE TABLE IF NOT EXISTS cash_flows (
                id INTEGER PRIMARY KEY,
                date DATE NOT NULL,
                amount DECIMAL(15,2) NOT NULL,
                flow_type TEXT NOT NULL
            )
            '''
        ]
        
        with self.conn:
            cursor = self.conn.cursor()
            for statement in statements:
                cursor.execute(statement)
            self.conn.commit()

    def tables_exist(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        table_names = [table['name'] for table in tables]
        required_tables = ['price_cache', 'transactions', 'cash_flows']
        return all(table in table_names for table in required_tables)

class DateValidator:
    @staticmethod
    def validate_not_future(check_date: date, operation_name: str):
        today = date.today()
        if check_date > today:
            raise ValueError(f"Error: Cannot perform {operation_name} with future date {check_date}. Today is {today}.")

    @staticmethod
    def validate_date_range(start_date: date, end_date: date, operation_name: str):
        today = date.today()
        if start_date > today or end_date > today:
            raise ValueError(f"Error: Cannot perform {operation_name} with future dates. Date range {start_date} to {end_date} contains future dates. Today is {today}.")
        if start_date > end_date:
            raise ValueError(f"Error: Invalid date range for {operation_name}. Start date {start_date} is after end date {end_date}.")


class StockPriceCache:
    def __init__(self, db_file: str):
        self.db_manager = DatabaseManager(db_file)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)  # Set to DEBUG level
        self.parse_date = parse_date

    def get_cached_prices(self, symbol: str, start_date: InputDate, end_date: InputDate) -> pd.Series:
        start_date = self.parse_date(start_date)
        end_date = self.parse_date(end_date)
        self.logger.debug(f"Getting cached prices for {symbol} from {start_date} to {end_date}")
        with self.db_manager as cursor:
            cursor.execute(
                "SELECT date, price FROM price_cache WHERE symbol = ? AND date BETWEEN ? AND ? ORDER BY date",
                (symbol, start_date.isoformat(), end_date.isoformat())
            )
            results = cursor.fetchall()
        self.logger.debug(f"Raw results from database: {results}")
        prices = pd.Series({pd.to_datetime(row['date']): row['price'] for row in results}, name=symbol)
        prices = prices.sort_index()
        self.logger.debug(f"Retrieved prices:\n{prices}")
        return prices

    def cache_prices(self, symbol: str, prices: pd.Series):
        self.logger.debug(f"Caching prices for {symbol}: {prices}")
        with self.db_manager as cursor:
            for index, price in prices.items():
                if isinstance(index, (datetime, date)):
                    date_str = index.strftime('%Y-%m-%d')
                else:
                    date_str = str(index)
                cursor.execute(
                    "INSERT OR REPLACE INTO price_cache (symbol, date, price) VALUES (?, ?, ?)",
                    (symbol, date_str, float(price))
                )
                self.logger.debug(f"Cached price for {symbol} on {date_str}: {price}")
        self.logger.debug(f"Cached {len(prices)} prices for {symbol}")

    def get_last_cached_date(self, symbol: str) -> Optional[date]:
        self.logger.debug(f"Getting last cached date for {symbol}")
        with self.db_manager as cursor:
            cursor.execute(
                "SELECT MAX(date) FROM price_cache WHERE symbol = ?",
                (symbol,)
            )
            result = cursor.fetchone()
        if result and result[0]:
            last_date = self.parse_date(result[0])
            self.logger.debug(f"Last cached date for {symbol}: {last_date}")
            return last_date
        else:
            self.logger.debug(f"No cached dates found for {symbol}")
            return None


class PortfolioAnalyzer:
    def __init__(self, portfolio: 'Portfolio'):
        self.portfolio = portfolio
        self.logger = logging.getLogger(__name__)

    def calculate_roi(self, start_date: date, end_date: date) -> float:
        if start_date > end_date:
            raise ValueError("Start date must be before end date")
        start_value = self.portfolio._get_portfolio_value_at_date(start_date)
        end_value = self.portfolio._get_portfolio_value_at_date(end_date)
        if start_value == 0:
            return 0  # Avoid division by zero
        return (end_value - start_value) / start_value * 100

    def calculate_spy_roi(self, start_date: date, end_date: date) -> Optional[float]:
        try:
            self.logger.info(f"Calculating S&P 500 ROI from {start_date} to {end_date}")
            
            spy_data = self.portfolio._fetch_prices(['SPY'], start_date, end_date)
            if spy_data.empty or 'SPY' not in spy_data.columns:
                self.logger.warning("Unable to fetch S&P 500 data for ROI calculation")
                return None
            
            spy_start_price = spy_data['SPY'].iloc[0]
            spy_end_price = spy_data['SPY'].iloc[-1]
            self.logger.info(f"S&P 500 start price: {spy_start_price}, end price: {spy_end_price}")
            
            spy_roi = ((spy_end_price - spy_start_price) / spy_start_price) * 100
            self.logger.info(f"Calculated S&P 500 ROI: {spy_roi}%")
            
            return spy_roi
        except Exception as e:
            self.logger.error(f"Error in calculate_spy_roi: {str(e)}")
            return None

    def compare_to_spy(self, start_date: date, end_date: date) -> Optional[float]:
        try:
            self.logger.info(f"Comparing portfolio to S&P 500 from {start_date} to {end_date}")
            portfolio_roi = self.calculate_roi(start_date, end_date)
            self.logger.info(f"Portfolio ROI: {portfolio_roi}")
            
            spy_data = self.portfolio._fetch_prices(['SPY'], start_date, end_date)
            if spy_data.empty:
                self.logger.warning("Unable to fetch S&P 500 data for comparison")
                return None
            
            spy_start_price = spy_data['SPY'].iloc[0]
            spy_end_price = spy_data['SPY'].iloc[-1]
            self.logger.info(f"S&P 500 start price: {spy_start_price}, end price: {spy_end_price}")
            
            spy_roi = ((spy_end_price - spy_start_price) / spy_start_price) * 100
            self.logger.info(f"S&P 500 ROI: {spy_roi}")
            
            difference = portfolio_roi - spy_roi
            self.logger.info(f"Difference: {difference}")
            
            return difference
        except Exception as e:
            self.logger.error(f"Error in compare_to_spy: {str(e)}")
            return None

class Portfolio:
    from typing import Optional

    def __init__(
            self,
            db_path: str = DEFAULT_DB_PATH,
            price_fetcher: Optional[PriceFetcher] = None,
            price_fetcher_name: Optional[str] = None
            ):
        self.logger = setup_logger(__name__, LOG_FILE_PATH, level=logging.DEBUG)
        self.logger.info("Initializing Portfolio object")

        self.db_path = db_path
        self._invalid_symbols: Set[str] = set()
        self.db_manager = DatabaseManager(db_path)
        with self.db_manager as _:
            if not self.db_manager.tables_exist():
                self.logger.warning("Database tables do not exist. Initializing...")
                self.db_manager.init_db()
            else:
                self.logger.info("Database tables already exist.")

        self.logger.debug("Creating StockPriceCache")
        self.price_cache = StockPriceCache(db_path)
        
        self.logger.debug("Creating PortfolioAnalyzer")
        self.analyzer = PortfolioAnalyzer(self)

        # Price fetcher initialization
        if price_fetcher:
            self.price_fetcher = price_fetcher
        elif price_fetcher_name:
            try:
                price_fetcher_manager = PriceFetcherManager('plugins')
                self.price_fetcher = price_fetcher_manager.get_price_fetcher(price_fetcher_name)
            except (ValueError, FileNotFoundError) as e:
                self.logger.warning(f"Error loading price fetcher '{price_fetcher_name}': {str(e)}. Falling back to YFinancePriceFetcher.")
                self.price_fetcher = YFinancePriceFetcher()
        else:
            self.price_fetcher = YFinancePriceFetcher()

        self.logger.debug("Loading transactions and cash flows")
        self.transactions = self.get_transaction_history()
        self.cash_flows = self.get_cash_flow_history()
        
        self.logger.info(f"Loaded {len(self.transactions)} transactions from the database")
        self.logger.info(f"Loaded {len(self.cash_flows)} cash flows from the database")
        
        self.print_database_content()
        
        self.logger.info("Portfolio object initialized")

    def _load_transactions_and_cash_flows(self):
        self.transactions = self.get_transaction_history()
        self.cash_flows = self.get_cash_flow_history()

    def _init_db(self):
        self.logger.debug("Entering _init_db method")
        with sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
            conn.execute("PRAGMA foreign_keys = 1")
            cursor = conn.cursor()
            self.logger.debug("Creating transactions table")
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS transactions (
                    id INTEGER PRIMARY KEY,
                    date DATE NOT NULL,
                    symbol TEXT NOT NULL,
                    action TEXT NOT NULL,
                    amount DECIMAL(15,2) NOT NULL,
                    price DECIMAL(15,2) NOT NULL,
                    fees DECIMAL(15,2) NOT NULL
                )
            ''')
            self.logger.debug("Creating cash_flows table")
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS cash_flows (
                    id INTEGER PRIMARY KEY,
                    date DATE NOT NULL,
                    amount DECIMAL(15,2) NOT NULL,
                    flow_type TEXT NOT NULL
                )
            ''')
            self.logger.debug("Creating price_cache table")
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS price_cache (
                    symbol TEXT,
                    date DATE,
                    price DECIMAL(15,2),
                    PRIMARY KEY (symbol, date)
                )
            ''')
            conn.commit()
        self.logger.debug("Exiting _init_db method")

    @property
    def invalid_symbols(self) -> Set[str]:
        return self._invalid_symbols

    @invalid_symbols.setter
    def invalid_symbols(self, value: Set[str]):
        self._invalid_symbols = value

    def reset_invalid_symbols(self):
        self.logger.info("Resetting invalid symbols")
        self._invalid_symbols = set()

    def add_transaction(self, transaction: Transaction) -> None:
        DateValidator.validate_not_future(transaction.date, "add transaction")
        self.logger.info(f"Attempting to add transaction: {transaction}")
        
        if transaction.action == 'Buy':
            current_cash = self.get_cash_balance()
            transaction_cost = transaction.amount + transaction.fees
            if transaction_cost > current_cash:
                error_msg = f"Insufficient funds. Current balance: ${current_cash:.2f}, Transaction cost: ${transaction_cost:.2f}"
                self.logger.error(error_msg)
                raise ValueError(error_msg)
        
        with sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO transactions (date, symbol, action, amount, price, fees)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (transaction.date, transaction.symbol, transaction.action,
                  transaction.amount, transaction.price, transaction.fees))
            conn.commit()
            
            # Verify the insertion
            cursor.execute('SELECT * FROM transactions WHERE rowid = last_insert_rowid()')
            inserted_row = cursor.fetchone()
            if inserted_row:
                self.logger.debug(f"Inserted transaction: date={inserted_row[1]}, symbol={inserted_row[2]}, action={inserted_row[3]}, amount={inserted_row[4]}, price={inserted_row[5]}, fees={inserted_row[6]}")
            else:
                self.logger.warning("Failed to retrieve the inserted transaction")
            
        self.transactions.append(transaction)
        self.logger.info(f"Transaction added successfully")

    def add_cash_flow(self, cash_flow: CashFlow) -> None:
        DateValidator.validate_not_future(cash_flow.date, "add cash flow")
        self.logger.info(f"Attempting to add cash flow: {cash_flow}")
        
        if cash_flow.flow_type == 'Withdrawal':
            current_cash = self.get_cash_balance()
            if cash_flow.amount > current_cash:
                error_msg = f"Insufficient funds for withdrawal. Current balance: ${current_cash:.2f}, Withdrawal amount: ${cash_flow.amount:.2f}"
                self.logger.error(error_msg)
                raise ValueError(error_msg)
        
        with sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO cash_flows (date, amount, flow_type)
                VALUES (?, ?, ?)
            ''', (cash_flow.date, cash_flow.amount, cash_flow.flow_type))
            conn.commit()
            
            # Verify the insertion
            cursor.execute('SELECT * FROM cash_flows WHERE rowid = last_insert_rowid()')
            inserted_row = cursor.fetchone()
            if inserted_row:
                self.logger.debug(f"Inserted cash flow: date={inserted_row[1]}, amount={inserted_row[2]}, flow_type={inserted_row[3]}")
            else:
                self.logger.warning("Failed to retrieve the inserted cash flow")
            
        self.cash_flows.append(cash_flow)
        self.logger.info(f"Cash flow added successfully")

    def _get_stock_price(self, symbol: str, date: date) -> Optional[float]:
        self.logger.debug(f"Attempting to get stock price for {symbol} on {date}")

        if symbol in self._invalid_symbols:
            self.logger.warning(f"{symbol} is invalid. Skipping.")
            return None

        cached_prices = self.price_cache.get_cached_prices(symbol, date, date)
        self.logger.debug(f"Cached prices for {symbol} on {date}: {cached_prices}")
        
        cached_price = cached_prices.get(pd.Timestamp(date))
        if cached_price is not None:
            self.logger.debug(f"Price for {symbol} on {date} found in cache: {cached_price}")
            return cached_price

        today = datetime.now().date()
        if date > today:
            self.logger.warning(f"Requested date {date} is in the future. Using latest available price.")
            date = today
        elif date == today:
            self.logger.info(f"Fetching current price for {symbol}")
            current_price = self.price_fetcher.fetch_current_price(symbol)
            if current_price is not None:
                self.price_cache.cache_prices(symbol, pd.Series({pd.Timestamp(date): current_price}))
                return current_price

        start_date = date - pd.Timedelta(days=5)
        end_date = date

        self.logger.debug(f"Fetching prices for {symbol} from {start_date} to {end_date}")
        price_data = self._fetch_prices([symbol], start_date, end_date)
        self.logger.debug(f"Fetched price data: {price_data}")

        if not price_data.empty and symbol in price_data.columns:            
            # Ensure the index is in datetime format
            price_data.index = pd.to_datetime(price_data.index)

            # Filter the DataFrame where the date part of the index is less than or equal to the given date
            valid_data = price_data[price_data.index.date <= date]

            if not valid_data.empty:
                self.logger.debug(f"Valid data for {symbol}: {valid_data}")
                self.price_cache.cache_prices(symbol, valid_data[symbol])
                price = valid_data[symbol].iloc[-1]
                last_date = valid_data.index[-1].date()
                self.logger.debug(f"Price found for {symbol}: {price:.2f} on {last_date}")
                return price
            else:
                self.logger.warning(f"No valid data found for {symbol} on or before {date}")
        else:
            self.logger.warning(f"No price data available for {symbol} between {start_date} and {end_date}")

        return self._get_last_known_price(symbol)

    def get_current_value(self) -> float:
        try:
            cash_balance = self.get_cash_balance()
            stock_value = self._get_total_stock_value()
            total_value = round(cash_balance + stock_value, 2)
            self.logger.info(f"Current portfolio value: ${total_value:.2f} (Cash: ${cash_balance:.2f}, Stocks: ${stock_value:.2f})")
            return total_value
        except PriceDataError as e:
            self.logger.error(f"Error calculating stock value: {str(e)}")
            return cash_balance  # Return only cash balance if stock value can't be calculated
        except Exception as e:
            self.logger.error(f"Unexpected error calculating portfolio value: {str(e)}", exc_info=True)
            raise

    def _get_last_known_price(self, symbol: str) -> Optional[float]:
        with self.db_manager as cursor:
            cursor.execute(
                "SELECT price FROM price_cache WHERE symbol = ? ORDER BY date DESC LIMIT 1",
                (symbol,)
            )
            result = cursor.fetchone()
        if result:
            self.logger.info(f"Last known price for {symbol}: ${result[0]:.2f}")
            return result[0]
        else:
            self.logger.warning(f"No last known price found for {symbol}")
            return None

    def _get_portfolio_value_at_date(self, target_date: date) -> float:
        value = self._get_cash_at_date(target_date)
        holdings = self._get_holdings_at_date(target_date)
        
        if holdings:
            symbols = list(holdings.keys())
            prices = self._fetch_prices(symbols, target_date, target_date)
            if not prices.empty:
                for symbol, shares in holdings.items():
                    if symbol in prices.columns:
                        value += shares * prices[symbol].iloc[0]
            else:
                self.logger.warning(f"Unable to fetch prices for portfolio valuation on {target_date}")

        return value

    def _get_cash_at_date(self, target_date: date) -> float:
        with sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 
                    COALESCE(SUM(CASE WHEN flow_type IN ('Deposit', 'Dividend') THEN amount 
                                      WHEN flow_type = 'Withdrawal' THEN -amount 
                                      ELSE 0 END), 0) as cash_flow_sum,
                    COALESCE(SUM(CASE WHEN action = 'Buy' THEN -amount ELSE amount END), 0) as transaction_sum
                FROM (
                    SELECT date, 0 as amount, '' as flow_type, action, amount as transaction_amount
                    FROM transactions
                    WHERE date <= ?
                    UNION ALL
                    SELECT date, amount, flow_type, '' as action, 0 as transaction_amount
                    FROM cash_flows
                    WHERE date <= ?
                )
            ''', (target_date, target_date))
            result = cursor.fetchone()
            cash_balance = result[0] + result[1]
            if cash_balance < 0:
                raise ValueError(f"Error: Negative cash balance of ${cash_balance:.2f} detected on {target_date}")
            return cash_balance

    def _get_holdings_at_date(self, target_date: date) -> Dict[str, float]:
        with sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT symbol, SUM(CASE WHEN action = 'Buy' THEN amount / price ELSE -amount / price END) as net_shares
                FROM transactions
                WHERE date <= ?
                GROUP BY symbol
                HAVING net_shares > 0
            ''', (target_date,))
            return {row[0]: row[1] for row in cursor.fetchall()}

    def get_transaction_history(self) -> List[Transaction]:
        self.logger.info("Retrieving transaction history")
        transactions = []
        try:
            with sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM transactions ORDER BY date')
                rows = cursor.fetchall()
                self.logger.info(f"Retrieved {len(rows)} transaction records from database")
                for row in rows:
                    self.logger.debug(f"Raw transaction data: {dict(row)}")
                    try:
                        transaction = Transaction(
                            date=row['date'],
                            symbol=row['symbol'],
                            action=row['action'],
                            amount=float(row['amount']),
                            price=float(row['price']),
                            fees=float(row['fees'])
                        )
                        transactions.append(transaction)
                    except Exception as e:
                        self.logger.error(f"Error creating Transaction object: {str(e)}")
        except sqlite3.OperationalError as e:
            self.logger.error(f"Error retrieving transactions: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unexpected error in get_transaction_history: {str(e)}")
        
        self.logger.info(f"Returning {len(transactions)} transaction objects")
        return transactions

    def get_cash_flow_history(self) -> List[CashFlow]:
        self.logger.info("Retrieving cash flow history")
        with sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM cash_flows ORDER BY date')
            cash_flows = []
            rows = cursor.fetchall()
            self.logger.info(f"Retrieved {len(rows)} cash flow records from database")
            for row in rows:
                self.logger.debug(f"Raw cash flow data: {dict(row)}")
                try:
                    cash_flow = CashFlow(
                        date=row['date'],
                        amount=float(row['amount']),
                        flow_type=row['flow_type']
                    )
                    cash_flows.append(cash_flow)
                except Exception as e:
                    self.logger.error(f"Error creating CashFlow object: {str(e)}")
            self.logger.info(f"Returning {len(cash_flows)} cash flow objects")
            return cash_flows

    def print_database_content(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM cash_flows")
            cash_flows = cursor.fetchall()
            self.logger.info(f"Cash Flows in database: {cash_flows}")
            cursor.execute("SELECT * FROM transactions")
            transactions = cursor.fetchall()
            self.logger.info(f"Transactions in database: {transactions}")
        self.logger.info(f"Transactions in memory: {self.transactions}")
        self.logger.info(f"Cash flows in memory: {self.cash_flows}")

    def get_current_holdings(self) -> Dict[str, float]:
        holdings = {}
        for t in self.get_transaction_history():
            if t.action == 'Buy':
                holdings[t.symbol] = holdings.get(t.symbol, 0) + (t.amount / t.price)
            else:  # Sell
                holdings[t.symbol] = holdings.get(t.symbol, 0) - (t.amount / t.price)
        
        # Remove any symbols with zero or negative shares
        holdings = {symbol: shares for symbol, shares in holdings.items() if shares > 0}
        
        self.logger.info(f"Current holdings: {holdings}")
        return holdings

    def get_cash_balance(self) -> float:
        with sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
            cursor = conn.cursor()
            
            # Calculate cash flow sum
            cursor.execute('''
                SELECT COALESCE(SUM(CASE WHEN flow_type IN ('Deposit', 'Dividend') THEN amount 
                                        WHEN flow_type = 'Withdrawal' THEN -amount 
                                        ELSE 0 END), 0) as cash_flow_sum
                FROM cash_flows
            ''')
            cash_flow_sum = cursor.fetchone()[0]
            self.logger.debug(f"Cash flow sum: {cash_flow_sum:.2f}")
            
            # Calculate transaction sum
            cursor.execute('''
                SELECT COALESCE(SUM(CASE WHEN action = 'Buy' THEN -amount - fees ELSE amount - fees END), 0) as transaction_sum
                FROM transactions
            ''')
            transaction_sum = cursor.fetchone()[0]
            self.logger.debug(f"Transaction sum: {transaction_sum:.2f}")
            
            cash_balance = round(Decimal(cash_flow_sum) + Decimal(transaction_sum), 2)
            self.logger.debug(f"Calculated cash balance: {cash_balance:.2f}")
            
            if cash_balance < 0:
                raise ValueError(f"Error: Negative cash balance of ${cash_balance:.2f} detected")
            return float(cash_balance)

    def get_portfolio_value_over_time(self, start_date: InputDate, end_date: InputDate) -> pd.Series:
        try:
            start_date = parse_date(start_date)
            end_date = parse_date(end_date)
            DateValidator.validate_date_range(start_date, end_date, "get portfolio value")

            dates = pd.date_range(start=start_date, end=end_date)
            values = []
            
            transactions = sorted([t for t in self.get_transaction_history() if start_date <= t.date <= end_date], key=lambda x: x.date)
            cash_flows = sorted([cf for cf in self.get_cash_flow_history() if start_date <= cf.date <= end_date], key=lambda x: x.date)
            
            symbols = set(t.symbol for t in transactions)
            price_data = self._fetch_prices(list(symbols), start_date, end_date)
            
            cumulative_cash = Decimal('0')
            holdings = {symbol: Decimal('0') for symbol in symbols}
            
            cf_index = 0
            t_index = 0
            
            for date in dates:
                current_date = date.date()
                
                # Process cash flows for the current date
                while cf_index < len(cash_flows) and cash_flows[cf_index].date == current_date:
                    cf = cash_flows[cf_index]
                    if cf.flow_type in ('Deposit', 'Dividend'):
                        cumulative_cash += Decimal(str(cf.amount))
                    else:
                        cumulative_cash -= Decimal(str(cf.amount))
                    self.logger.debug(f"Processed cash flow: {cf}, New cash: {cumulative_cash}")
                    cf_index += 1
                
                # Process transactions for the current date
                while t_index < len(transactions) and transactions[t_index].date == current_date:
                    t = transactions[t_index]
                    if t.action == 'Buy':
                        holdings[t.symbol] += Decimal(str(t.amount / t.price))
                        cumulative_cash -= Decimal(str(t.amount + t.fees))
                    else:  # Sell
                        holdings[t.symbol] -= Decimal(str(t.amount / t.price))
                        cumulative_cash += Decimal(str(t.amount - t.fees))
                    self.logger.debug(f"Processed transaction: {t}, New holdings: {holdings}, Cash: {cumulative_cash}")
                    t_index += 1
                
                stock_value = Decimal('0')
                for symbol, shares in holdings.items():
                    if not price_data.empty and symbol in price_data.columns:
                        price_series = price_data[symbol]
                        price = price_series.loc[date] if date in price_series.index else None
                        if price is not None:
                            price = Decimal(str(price))
                            self.logger.debug(f"Using fetched price for {symbol} on {current_date}: ${float(price):.2f}")
                        else:
                            self.logger.warning(f"No price data for {symbol} on {current_date}, using last known price")
                            price = Decimal(str(self._get_last_known_price(symbol) or 0))
                    else:
                        self.logger.warning(f"No price data for {symbol}, using last known price")
                        price = Decimal(str(self._get_last_known_price(symbol) or 0))
                    stock_value += shares * price
                
                total_value = round(cumulative_cash + stock_value, 2)
                values.append(float(total_value))
                self.logger.info(f"Date: {current_date}, Cash: {float(cumulative_cash):.2f}, Stock Value: {float(stock_value):.2f}, Total: {float(total_value):.2f}")
            
            return pd.Series(values, index=dates)

        except ValueError as e:
            raise PortfolioValueError(f"Date validation error: {str(e)}")
        except KeyError as e:
            raise PriceDataError(f"Missing price data for symbol: {str(e)}")
        except Exception as e:
            raise PortfolioValueError(f"Unexpected error in portfolio valuation: {str(e)}")

    def _fetch_prices(self, symbols: List[str], start_date: InputDate, end_date: InputDate) -> pd.DataFrame:
        try:
            self.logger.info(f"Fetching prices for symbols: {symbols} from {start_date} to {end_date}")
            
            start_date = parse_date(start_date)
            end_date = parse_date(end_date)
            price_data = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date))
            
            for symbol in symbols:
                cached_prices = self.price_cache.get_cached_prices(symbol, start_date, end_date)
                self.logger.info(f"Cached data for {symbol}: {len(cached_prices)} entries")
                
                if len(cached_prices) < (end_date - start_date).days + 1:
                    fetch_start_date = start_date
                    if not cached_prices.empty:
                        last_cached_date = cached_prices.index.max().date()
                        fetch_start_date = last_cached_date + timedelta(days=1)

                    self.logger.debug(f"Fetching missing data for {symbol} from {fetch_start_date} to {end_date}")
                    fetched_prices = self.price_fetcher.fetch_prices([symbol], fetch_start_date, end_date)
                    self.logger.debug(f"Fetched prices: {fetched_prices}")
                    
                    if not fetched_prices.empty:
                        self.logger.info(f"Fetched {len(fetched_prices)} new prices for {symbol}")
                        self.price_cache.cache_prices(symbol, fetched_prices[symbol])
                        cached_prices = self.price_cache.get_cached_prices(symbol, start_date, end_date)
                    else:
                        self.logger.warning(f"No new price data fetched for {symbol}")
                
                if not cached_prices.empty:
                    price_data[symbol] = cached_prices
                else:
                    self.logger.warning(f"No price data available for {symbol}. Using last known price.")
                    last_price = self._get_last_known_price(symbol)
                    if last_price is not None:
                        price_data[symbol] = last_price
                    else:
                        self.logger.error(f"No historical price data available for {symbol}")
                        raise PriceDataError(f"No price data available for {symbol}")

            if price_data.empty:
                self.logger.warning("No price data available for any symbol")
                return pd.DataFrame(index=pd.date_range(start=start_date, end=end_date))

            #self.logger.info(f"Final price data shape: {price_data.shape}")
            self.logger.debug(f"Final price data:\n{price_data}")
            return price_data.ffill().infer_objects()
        except Exception as e:
            self.logger.error(f"Error fetching prices: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise PriceDataError(f"Failed to fetch prices: {str(e)}")

    def _get_cached_prices(self, symbols: List[str], start_date: date, end_date: date) -> pd.DataFrame:
        date_range = pd.date_range(start=start_date, end=end_date)
        price_data = pd.DataFrame(index=date_range, columns=symbols)
        
        for symbol in symbols:
            symbol_prices = self.price_cache.get_cached_prices(symbol, start_date, end_date)
            price_data[symbol] = symbol_prices
            self.logger.info(f"Cached data for {symbol}: {len(symbol_prices)} entries")
        
        return price_data

    def _get_missing_data_symbols(self, price_data: pd.DataFrame) -> List[str]:
        missing_data = price_data.isna().any()
        return missing_data[missing_data].index.tolist()

    def _fetch_missing_data(self, symbols: List[str], start_date: date, end_date: date) -> pd.DataFrame:
        current_date = datetime.now().date()
        fetch_end_date = min(end_date, current_date)
        fetch_start_date = max(start_date, fetch_end_date - timedelta(days=365))
        
        return self.price_fetcher.fetch_prices(symbols, fetch_start_date, fetch_end_date)

    def _update_price_data(self, price_data: pd.DataFrame, fetched_data: pd.DataFrame) -> pd.DataFrame:
        for symbol in fetched_data.columns:
            price_data.loc[fetched_data.index[0]:fetched_data.index[-1], symbol] = fetched_data[symbol]
            for date, price in fetched_data[symbol].items():
                if not pd.isna(price):
                    self.price_cache.cache_prices(symbol, pd.Series({date: price}))
        return price_data

    def _get_total_stock_value(self) -> float:
        current_date = datetime.now().date()
        total_stock_value = Decimal('0')
        holdings = self.get_current_holdings()
        
        if not holdings:
            self.logger.info("No current holdings found.")
            return float(total_stock_value)

        symbols = list(holdings.keys())
        self.logger.info(f"Fetching prices for symbols: {symbols}")
        try:
            prices = self._fetch_prices(symbols, current_date, current_date)
        except PriceDataError as e:
            self.logger.error(f"Error fetching prices: {str(e)}")
            self.logger.info("Falling back to last known prices")
            prices = self._get_last_known_prices(symbols)

        for symbol, shares in holdings.items():
            try:
                if not prices.empty and symbol in prices.columns:
                    price = Decimal(str(prices.iloc[-1][symbol]))  # Get the last (most recent) price
                    self.logger.info(f"Using fetched price for {symbol}: ${float(price):.2f}")
                else:
                    price = Decimal(str(self._get_last_known_price(symbol)))
                    self.logger.warning(f"Using last known price for {symbol}: ${float(price):.2f}")
                
                stock_value = Decimal(str(shares)) * price
                total_stock_value += stock_value
                self.logger.info(f"Value for {symbol}: {shares} shares @ ${float(price):.2f} = ${float(stock_value):.2f}")
            except Exception as e:
                self.logger.error(f"Error calculating value for {symbol}: {str(e)}")
                # Continue with the next symbol instead of raising an exception

        self.logger.info(f"Total stock value: ${float(total_stock_value):.2f}")
        return float(total_stock_value)

    def _get_last_known_prices(self, symbols: List[str]) -> pd.Series:
            prices = {}
            for symbol in symbols:
                price = self._get_last_known_price(symbol)
                if price is not None:
                    prices[symbol] = price
                else:
                    self.logger.warning(f"No last known price found for {symbol}")
            return pd.Series(prices)

    def _get_purchase_price(self, symbol: str) -> float:
        with sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT price
                FROM transactions
                WHERE symbol = ? AND action = 'Buy'
                ORDER BY date DESC
                LIMIT 1
            ''', (symbol,))
            result = cursor.fetchone()
            return result[0] if result else 0.0

    def edit_transaction(self, index: int, new_transaction: Transaction) -> None:
        with sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE transactions
                SET date = ?, symbol = ?, action = ?, amount = ?, price = ?, fees = ?
                WHERE id = (SELECT id FROM transactions ORDER BY date LIMIT 1 OFFSET ?)
            ''', (new_transaction.date, new_transaction.symbol, new_transaction.action,
                  new_transaction.amount, new_transaction.price, new_transaction.fees, index))
            if cursor.rowcount == 0:
                raise ValueError("Invalid transaction index")
            conn.commit()

    def delete_transaction(self, index: int) -> None:
        with sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM transactions
                WHERE id = (SELECT id FROM transactions ORDER BY date LIMIT 1 OFFSET ?)
            ''', (index,))
            if cursor.rowcount == 0:
                raise ValueError("Invalid transaction index")
            conn.commit()

    def edit_cash_flow(self, index: int, new_cash_flow: CashFlow) -> None:
        with sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE cash_flows
                SET date = ?, amount = ?, flow_type = ?
                WHERE id = (SELECT id FROM cash_flows ORDER BY date LIMIT 1 OFFSET ?)
            ''', (new_cash_flow.date, new_cash_flow.amount, new_cash_flow.flow_type, index))
            if cursor.rowcount == 0:
                raise ValueError("Invalid cash flow index")
            conn.commit()

    def delete_cash_flow(self, index: int) -> None:
        with sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM cash_flows
                WHERE id = (SELECT id FROM cash_flows ORDER BY date LIMIT 1 OFFSET ?)
            ''', (index,))
            if cursor.rowcount == 0:
                raise ValueError("Invalid cash flow index")
            conn.commit()

    @property
    def cash(self):
        return self.get_cash_balance()
