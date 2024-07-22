from PySide6.QtWidgets import QWidget, QVBoxLayout, QLabel, QFrame
from PySide6.QtCore import Qt
from PySide6.QtCharts import QChart, QChartView, QLineSeries, QDateTimeAxis, QValueAxis
from PySide6.QtGui import QPainter
from datetime import date, datetime
from portfolio.portfolio import Portfolio
from typing import Dict, Any
import pandas as pd

def to_timestamp(date_value: Any) -> int:
    print(f"to_timestamp received: {date_value}, type: {type(date_value)}")
    
    if isinstance(date_value, str):
        dt = datetime.fromisoformat(date_value)
    elif isinstance(date_value, date):
        dt = datetime.combine(date_value, datetime.min.time())
    elif isinstance(date_value, datetime):
        dt = date_value
    elif isinstance(date_value, (int, float)):
        # Assume it's already a timestamp
        return int(date_value * 1000)
    elif hasattr(date_value, 'timestamp'):
        # If it has a timestamp method, use it
        return int(date_value.timestamp() * 1000)
    else:
        raise ValueError(f"Unexpected date type: {type(date_value)}")
    
    return int(dt.timestamp() * 1000)

class OverviewTab(QWidget):
    def __init__(self, portfolio: Portfolio):
        super().__init__()
        self.portfolio = portfolio
        self.create_widgets()

    def create_widgets(self):
        layout = QVBoxLayout(self)

        # Summary Frame
        summary_frame = QFrame(self)
        summary_frame.setFrameShape(QFrame.Shape.StyledPanel)
        summary_layout = QVBoxLayout(summary_frame)
        
        self.portfolio_value_label = QLabel("Current Portfolio Value: $0.00")
        self.cash_balance_label = QLabel("Cash Balance: $0.00")
        
        summary_layout.addWidget(self.portfolio_value_label)
        summary_layout.addWidget(self.cash_balance_label)
        
        layout.addWidget(summary_frame)

        # Chart
        self.chart_view = QChartView()
        self.chart_view.setRenderHint(QPainter.RenderHint.Antialiasing)
        layout.addWidget(self.chart_view)

        self.update()

    def update(self):
        current_value = self.portfolio.get_current_value()
        self.portfolio_value_label.setText(f"Current Portfolio Value: ${current_value:.2f}")
        self.cash_balance_label.setText(f"Cash Balance: ${self.portfolio.cash:.2f}")
        self.plot_portfolio_value()

    def plot_portfolio_value(self):
        end_date = datetime.now().date()
        start_date = self.get_start_date()
        
        if start_date == end_date:
            # No historical data to display
            return

        portfolio_values = self.portfolio.get_portfolio_value_over_time(start_date, end_date)
        
        if portfolio_values.empty:
            # No data to display
            return

        print(f"Type of portfolio_values: {type(portfolio_values)}")
        if isinstance(portfolio_values, pd.Series):
            print(f"Index type: {type(portfolio_values.index)}")
            print(f"Sample index item: {portfolio_values.index[0]}, type: {type(portfolio_values.index[0])}")
            print(f"Sample value: {portfolio_values.iloc[0]}, type: {type(portfolio_values.iloc[0])}")
            series = QLineSeries()
            for date, value in portfolio_values.items():
                # Convert to milliseconds since epoch, regardless of the exact type
                if isinstance(date, (pd.Timestamp, datetime)):
                    ms = int(date.timestamp() * 1000)
                elif isinstance(date, str):
                    ms = int(pd.Timestamp(date).timestamp() * 1000)
                else:
                    print(f"Unexpected date type: {type(date)}")
                    continue
                
                series.append(ms, float(value))

            chart = QChart()
            chart.addSeries(series)
            chart.setTitle("Portfolio Value Over Time")

            axis_x = QDateTimeAxis()
            axis_x.setFormat("dd-MM-yyyy")
            axis_x.setTitleText("Date")
            chart.addAxis(axis_x, Qt.AlignmentFlag.AlignBottom)
            series.attachAxis(axis_x)

            axis_y = QValueAxis()
            axis_y.setTitleText("Value ($)")
            chart.addAxis(axis_y, Qt.AlignmentFlag.AlignLeft)
            series.attachAxis(axis_y)

            self.chart_view.setChart(chart)
        else:
            print("No portfolio value data available to display")

    def get_start_date(self):
        all_dates = ([t.date for t in self.portfolio.transactions] +
                    [cf.date for cf in self.portfolio.cash_flows])
        return min(all_dates) if all_dates else date.today()
