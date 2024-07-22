from datetime import datetime, date as date_type
from dataclasses import dataclass
from typing import Union

@dataclass
class Transaction:
    """
    Represents a stock transaction.

    Attributes:
        date (date_type): The date of the transaction.
        symbol (str): The stock symbol.
        action (Literal['Buy', 'Sell']): The type of transaction (Buy or Sell).
        amount (float): The amount of money involved in the transaction.
        price (float): The price per share.
        fees (float): Any associated fees with the transaction.
    """
    date: date_type
    symbol: str
    action: str
    amount: float
    price: float
    fees: float = 0.0

    @property
    def shares(self) -> float:
        """Calculate the number of shares based on the amount and price."""
        return self.amount / self.price if self.price != 0 else 0

    def __post_init__(self):
        """Validate the transaction data after initialization."""
        self._validate_inputs()

    def _validate_inputs(self):
        """Validate the input data for the transaction."""
        if not isinstance(self.date, date_type):
            raise ValueError("Date must be a date object")
        if not isinstance(self.symbol, str) or not self.symbol:
            raise ValueError("Symbol must be a non-empty string")
        if self.action not in ['Buy', 'Sell']:
            raise ValueError("Action must be either 'Buy' or 'Sell'")
        if not isinstance(self.amount, (int, float)) or self.amount <= 0:
            raise ValueError("Amount must be a positive number")
        if not isinstance(self.price, (int, float)) or self.price <= 0:
            raise ValueError("Price must be a positive number")
        if not isinstance(self.fees, (int, float)) or self.fees < 0:
            raise ValueError("Fees must be a non-negative number")

    def total_value(self) -> float:
        """Calculate the total value of the transaction."""
        return round(self.amount + self.fees, 2)

    def net_value(self) -> float:
        """Calculate the net value of the transaction (excluding fees)."""
        return round(self.amount, 2)

    def to_dict(self) -> dict:
        """Convert the transaction to a dictionary."""
        return {
            'date': self.date.isoformat(),
            'symbol': self.symbol,
            'action': self.action,
            'amount': self.amount,
            'price': self.price,
            'fees': self.fees
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Transaction':
        """Create a Transaction instance from a dictionary."""
        data['date'] = datetime.fromisoformat(data['date']).date()
        return cls(**data)

    def __str__(self) -> str:
        """Return a string representation of the transaction."""
        return (f"{self.action} ${self.amount:.2f} of {self.symbol} "
                f"at ${self.price:.2f} per share on {self.date.strftime('%Y-%m-%d')}")

@dataclass
class CashFlow:
    """
    Represents a cash flow transaction.

    Attributes:
        date (datetime.date): The date of the cash flow.
        amount (float): The amount of the cash flow.
        flow_type (Literal['Deposit', 'Withdrawal', 'Dividend']): The type of cash flow.
    """
    date: date_type
    amount: float
    flow_type: str

    def __post_init__(self):
        """Validate the cash flow data after initialization."""
        self._validate_inputs()

    def _validate_inputs(self):
        """Validate the input data for the cash flow."""
        if not isinstance(self.date, date_type):
            raise ValueError("Date must be a datetime.date object")
        if not isinstance(self.amount, (int, float)) or self.amount <= 0:
            raise ValueError("Amount must be a positive number")
        if self.flow_type not in ['Deposit', 'Withdrawal', 'Dividend']:
            raise ValueError("Flow type must be 'Deposit', 'Withdrawal', or 'Dividend'")

    def to_dict(self) -> dict:
        """Convert the cash flow to a dictionary."""
        return {
            'date': self.date.isoformat(),
            'amount': self.amount,
            'flow_type': self.flow_type
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'CashFlow':
        """Create a CashFlow instance from a dictionary."""
        data['date'] = datetime.fromisoformat(data['date']).date()
        return cls(**data)

    def __str__(self) -> str:
        """Return a string representation of the cash flow."""
        return f"{self.flow_type} of ${self.amount:.2f} on {self.date.strftime('%Y-%m-%d')}"

def create_transaction(
        date: Union[str, datetime, date_type],
        symbol: str, action: str,
        amount: Union[int, float, str],
        price: Union[float, str], 
        fees: Union[float, str] = '0.0'
        ) -> Transaction:
    """
    Create a Transaction instance with input validation and type conversion.

    Args:
        date (Union[str, datetime, date_type]): The date of the transaction (YYYY-MM-DD if string).
        symbol (str): The stock symbol.
        action (str): The type of transaction ('Buy' or 'Sell').
        amount (Union[int, float, str]): The amount of money for the transaction.
        price (Union[float, str]): The price per share.
        fees (Union[float, str], optional): Any associated fees. Defaults to '0.0'.

    Returns:
        Transaction: A new Transaction instance.

    Raises:
        ValueError: If any of the inputs are invalid.
    """
    if isinstance(date, str):
        date = datetime.strptime(date, '%Y-%m-%d').date()
    elif isinstance(date, datetime):
        date = date.date()
    else:
        raise ValueError("Date must be a string, datetime, or date object")
    
    amount = float(amount)
    price = float(price)
    fees = float(fees)

    return Transaction(date, symbol, action, amount, price, fees)

def create_cash_flow(
        date: Union[str, datetime, date_type],
        amount: Union[float, str],
        flow_type: str
        ) -> CashFlow:
    """
    Create a CashFlow instance with input validation and type conversion.

    Args:
        date (Union[str, datetime, date_type]): The date of the cash flow (YYYY-MM-DD if string).
        amount (Union[float, str]): The amount of the cash flow.
        flow_type (str): The type of cash flow ('Deposit', 'Withdrawal', or 'Dividend').

    Returns:
        CashFlow: A new CashFlow instance.

    Raises:
        ValueError: If any of the inputs are invalid.
    """
    if isinstance(date, str):
        date = datetime.strptime(date, '%Y-%m-%d').date()
    elif isinstance(date, datetime):
        date = date.date()
    else:
        raise ValueError("Date must be a string, datetime, or date object")
    
    amount = float(amount)

    return CashFlow(date, amount, flow_type)
