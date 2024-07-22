from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QFormLayout, 
                               QLineEdit, QPushButton, QLabel, QCalendarWidget, 
                               QDialog, QMessageBox)
from PySide6.QtCore import Qt, QDate
from PySide6.QtCharts import QChart, QChartView, QLineSeries, QDateTimeAxis, QValueAxis
from PySide6.QtGui import QPainter, QColor
from portfolio.portfolio import Portfolio
from datetime import datetime, date
from core.date_utils import parse_date, DateValidator
from core.config import CURRENT_DATE, DEFAULT_CHART_HEIGHT, DEFAULT_CHART_WIDTH
import pandas as pd
from typing import Optional

class AnalysisTab(QWidget):
    def __init__(self, portfolio: Portfolio):
        super().__init__()
        self.portfolio = portfolio
        self.create_widgets()

    def create_widgets(self):
        layout = QVBoxLayout(self)

        # Input frame
        input_frame = QWidget()
        input_layout = QFormLayout(input_frame)

        self.start_date = QLineEdit()
        self.end_date = QLineEdit()

        start_date_button = QPushButton("Select Start Date")
        start_date_button.clicked.connect(lambda: self.show_calendar(self.start_date))
        end_date_button = QPushButton("Select End Date")
        end_date_button.clicked.connect(lambda: self.show_calendar(self.end_date))

        input_layout.addRow("Start Date:", self.start_date)
        input_layout.addRow(start_date_button)
        input_layout.addRow("End Date:", self.end_date)
        input_layout.addRow(end_date_button)

        button_layout = QHBoxLayout()
        self.roi_button = QPushButton("Calculate ROI")
        self.roi_button.clicked.connect(self.calculate_roi)
        self.compare_button = QPushButton("Compare to S&P 500")
        self.compare_button.clicked.connect(self.compare_to_spy)
        button_layout.addWidget(self.roi_button)
        button_layout.addWidget(self.compare_button)

        input_layout.addRow(button_layout)

        layout.addWidget(input_frame)

        # Results frame
        self.result_label = QLabel("Analysis results will appear here.")
        self.result_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.result_label.setStyleSheet("font-size: 14px;")
        layout.addWidget(self.result_label)

        # Chart
        self.chart_view = QChartView()
        self.chart_view.setRenderHint(QPainter.RenderHint.Antialiasing)
        self.chart_view.setMinimumHeight(DEFAULT_CHART_HEIGHT)
        self.chart_view.setMinimumWidth(DEFAULT_CHART_WIDTH)
        layout.addWidget(self.chart_view)

    def show_calendar(self, line_edit: QLineEdit):
        dialog = QDialog(self)
        dialog.setWindowTitle("Select Date")
        layout = QVBoxLayout(dialog)
        calendar = QCalendarWidget(dialog)
        layout.addWidget(calendar)

        def on_date_selected():
            selected_date = calendar.selectedDate()
            line_edit.setText(selected_date.toString("yyyy-MM-dd"))
            dialog.accept()

        calendar.clicked.connect(on_date_selected)
        dialog.exec()

    def calculate_roi(self):
        try:
            start_date = parse_date(self.start_date.text())
            end_date = parse_date(self.end_date.text())
            DateValidator.validate_date_range(start_date, end_date, "ROI calculation")
            
            roi = self.portfolio.analyzer.calculate_roi(start_date, end_date)
            self.result_label.setText(f"Return on Investment (ROI): {roi:.2f}%")
        except ValueError as e:
            QMessageBox.warning(self, "Input Error", str(e))
        except Exception as e:
            QMessageBox.critical(self, "Calculation Error", f"An unexpected error occurred: {str(e)}")

    def compare_to_spy(self):
        try:
            start_date = parse_date(self.start_date.text())
            end_date = parse_date(self.end_date.text())
            DateValidator.validate_date_range(start_date, end_date, "S&P 500 comparison")
            
            portfolio_roi = self.portfolio.analyzer.calculate_roi(start_date, end_date)
            spy_roi = self.portfolio.analyzer.calculate_spy_roi(start_date, end_date)
            
            if spy_roi is None:
                raise ValueError("Unable to calculate S&P 500 comparison")

            difference = portfolio_roi - spy_roi
            performance_text = (
                f"Portfolio ROI: {portfolio_roi:.2f}%\n"
                f"S&P 500 ROI: {spy_roi:.2f}%\n"
                f"Difference: {difference:.2f}%\n"
                f"Your portfolio {'outperformed' if difference > 0 else 'underperformed'} "
                f"the S&P 500 by {abs(difference):.2f}%"
            )
            self.result_label.setText(performance_text)
            
            self.plot_roi_comparison(start_date, end_date, portfolio_roi, spy_roi)
        except ValueError as e:
            QMessageBox.warning(self, "Input Error", str(e))
        except Exception as e:
            QMessageBox.critical(self, "Calculation Error", f"An unexpected error occurred: {str(e)}")

    def plot_roi_comparison(self, start_date: date, end_date: date, portfolio_roi: float, spy_roi: float):
        chart = QChart()
        chart.setTitle("ROI Comparison: Portfolio vs S&P 500")

        portfolio_series = QLineSeries()
        portfolio_series.setName("Portfolio ROI")
        portfolio_series.append(start_date.toordinal(), 0)
        portfolio_series.append(end_date.toordinal(), portfolio_roi)

        spy_series = QLineSeries()
        spy_series.setName("S&P 500 ROI")
        spy_series.append(start_date.toordinal(), 0)
        spy_series.append(end_date.toordinal(), spy_roi)

        chart.addSeries(portfolio_series)
        chart.addSeries(spy_series)

        axis_x = QDateTimeAxis()
        axis_x.setFormat("dd-MM-yyyy")
        axis_x.setTitleText("Date")
        chart.addAxis(axis_x, Qt.AlignmentFlag.AlignBottom)
        portfolio_series.attachAxis(axis_x)
        spy_series.attachAxis(axis_x)

        axis_y = QValueAxis()
        axis_y.setTitleText("ROI (%)")
        chart.addAxis(axis_y, Qt.AlignmentFlag.AlignLeft)
        portfolio_series.attachAxis(axis_y)
        spy_series.attachAxis(axis_y)

        chart.legend().setVisible(True)
        chart.legend().setAlignment(Qt.AlignmentFlag.AlignBottom)

        self.chart_view.setChart(chart)

    @staticmethod
    def parse_date(date_string: str) -> date:
        try:
            return datetime.strptime(date_string, "%Y-%m-%d").date()
        except ValueError:
            raise ValueError(f"Invalid date format: {date_string}. Please use YYYY-MM-DD.")

    @staticmethod
    def validate_date_range(start_date: date, end_date: date):
        if start_date > end_date:
            raise ValueError("Start date must be before end date.")
        if end_date > date.today():
            raise ValueError("End date cannot be in the future.")

    def update(self):
        # This method can be used to refresh the analysis if needed
        pass