from PySide6.QtWidgets import QMainWindow, QTabWidget, QVBoxLayout, QWidget, QMessageBox
from PySide6.QtCore import QTimer
from portfolio.portfolio import Portfolio
from .overview_tab_pyside import OverviewTab
from .transactions_tab_pyside import TransactionsTab
from .analysis_tab_pyside import AnalysisTab
import logging

class PortfolioManagerGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Portfolio Manager")
        self.setGeometry(100, 100, 1000, 800)
        self.logger = self.setup_logger()
        self.portfolio = None
        self.create_loading_screen()
        QTimer.singleShot(0, self.load_portfolio)

    def setup_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def create_loading_screen(self):
        from PySide6.QtWidgets import QLabel, QProgressBar
        self.loading_widget = QWidget(self)
        layout = QVBoxLayout(self.loading_widget)
        self.loading_label = QLabel("Loading portfolio...", self.loading_widget)
        self.progress_bar = QProgressBar(self.loading_widget)
        self.progress_bar.setRange(0, 0)  # Indeterminate progress
        layout.addWidget(self.loading_label)
        layout.addWidget(self.progress_bar)
        self.setCentralWidget(self.loading_widget)

    def load_portfolio(self):
        try:
            self.portfolio = Portfolio('portfolio.db')
            self.create_widgets()
        except Exception as e:
            self.logger.error(f"Error loading portfolio: {str(e)}")
            QMessageBox.critical(self, "Error", f"Failed to load portfolio: {str(e)}")
            self.close()

    def create_widgets(self):
        if self.portfolio is None:
            raise ValueError("Portfolio not initialized")
        
        self.tab_widget = QTabWidget(self)
        self.overview_tab = OverviewTab(self.portfolio)
        self.transactions_tab = TransactionsTab(self.portfolio, self.update_ui)
        self.analysis_tab = AnalysisTab(self.portfolio)

        self.tab_widget.addTab(self.overview_tab, "Overview")
        self.tab_widget.addTab(self.transactions_tab, "Transactions")
        self.tab_widget.addTab(self.analysis_tab, "Analysis")

        self.setCentralWidget(self.tab_widget)

        # Update the UI after creating widgets
        self.update_ui()

    def update_ui(self):
        self.overview_tab.update()
        self.transactions_tab.update()
        self.analysis_tab.update()

    def closeEvent(self, event):
        try:
            # Since we're using SQLite, we don't need to explicitly save the portfolio
            event.accept()
        except Exception as e:
            self.logger.error(f"Error on closing: {str(e)}")
            QMessageBox.critical(self, "Error", f"An error occurred while closing the application: {str(e)}")
            event.accept()
