from typing import Union, List, Dict, Optional, TYPE_CHECKING
from datetime import date, datetime

if TYPE_CHECKING:
    from datetime import date, datetime
    DateType = Union[str, date, datetime]
else:
    DateType = Union[str, 'date', 'datetime']

DateType = Union[str, date, datetime]
PriceType = float
SymbolType = str
SharesType = float
AmountType = float

TransactionDict = Dict[str, Union[DateType, SymbolType, str, AmountType, PriceType]]
CashFlowDict = Dict[str, Union[DateType, AmountType, str]]

PriceData = Dict[SymbolType, Dict[DateType, PriceType]]