from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QFormLayout, 
                               QLineEdit, QComboBox, QPushButton, QTableWidget, 
                               QTableWidgetItem, QMessageBox, QCalendarWidget,
                               QDialog, QLabel, QGroupBox, QAbstractItemView)
from PySide6.QtCore import Qt, QDate
from portfolio.portfolio import Portfolio
from portfolio.transaction import Transaction, CashFlow, create_transaction, create_cash_flow
from datetime import datetime
import logging

class TransactionsTab(QWidget):
    def __init__(self, portfolio: Portfolio, update_callback):
        super().__init__()
        self.portfolio = portfolio
        self.update_callback = update_callback
        self.logger = logging.getLogger(__name__)
        self.create_widgets()

    def create_widgets(self):
        main_layout = QVBoxLayout(self)

        # Input forms
        input_layout = QHBoxLayout()
        input_layout.addWidget(self.create_transaction_input())
        input_layout.addWidget(self.create_cash_flow_input())
        main_layout.addLayout(input_layout)

        # History tables
        history_layout = QHBoxLayout()
        history_layout.addWidget(self.create_transaction_history())
        history_layout.addWidget(self.create_cash_flow_history())
        main_layout.addLayout(history_layout)

    def create_transaction_input(self):
        group_box = QGroupBox("Add Transaction")
        form = QFormLayout()
        self.transaction_entries = {
            'date': QLineEdit(),
            'symbol': QLineEdit(),
            'action': QComboBox(),
            'amount': QLineEdit(),
            'price': QLineEdit(),
            'fees': QLineEdit()
        }
        self.transaction_entries['action'].addItems(["Buy", "Sell"])
        
        date_button = QPushButton("Select Date")
        date_button.clicked.connect(lambda: self.show_calendar(self.transaction_entries['date']))
        
        form.addRow("Date:", self.transaction_entries['date'])
        form.addRow(date_button)
        form.addRow("Symbol:", self.transaction_entries['symbol'])
        form.addRow("Action:", self.transaction_entries['action'])
        form.addRow("Amount ($):", self.transaction_entries['amount'])
        form.addRow("Price per Share ($):", self.transaction_entries['price'])
        form.addRow("Fees ($):", self.transaction_entries['fees'])

        add_button = QPushButton("Add Transaction")
        add_button.clicked.connect(self.add_transaction)
        form.addRow(add_button)

        group_box.setLayout(form)
        return group_box

    def create_cash_flow_input(self):
        group_box = QGroupBox("Add Cash Flow")
        form = QFormLayout()
        self.cash_flow_entries = {
            'date': QLineEdit(),
            'amount': QLineEdit(),
            'flow_type': QComboBox()
        }
        self.cash_flow_entries['flow_type'].addItems(["Deposit", "Withdrawal", "Dividend"])
        
        date_button = QPushButton("Select Date")
        date_button.clicked.connect(lambda: self.show_calendar(self.cash_flow_entries['date']))
        
        form.addRow("Date:", self.cash_flow_entries['date'])
        form.addRow(date_button)
        form.addRow("Amount ($):", self.cash_flow_entries['amount'])
        form.addRow("Type:", self.cash_flow_entries['flow_type'])

        add_button = QPushButton("Add Cash Flow")
        add_button.clicked.connect(self.add_cash_flow)
        form.addRow(add_button)

        group_box.setLayout(form)
        return group_box

    def show_calendar(self, line_edit):
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

    def create_transaction_history(self):
        group_box = QGroupBox("Transaction History")
        layout = QVBoxLayout()
        self.transaction_table = QTableWidget(0, 7)
        self.transaction_table.setHorizontalHeaderLabels(["Date", "Symbol", "Action", "Amount ($)", "Price ($)", "Shares", "Fees ($)"])
        self.transaction_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.transaction_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        layout.addWidget(self.transaction_table)
        group_box.setLayout(layout)
        return group_box

    def create_cash_flow_history(self):
        group_box = QGroupBox("Cash Flow History")
        layout = QVBoxLayout()
        self.cash_flow_table = QTableWidget(0, 3)
        self.cash_flow_table.setHorizontalHeaderLabels(["Date", "Amount ($)", "Type"])
        self.cash_flow_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.cash_flow_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        layout.addWidget(self.cash_flow_table)
        group_box.setLayout(layout)
        return group_box

    def add_transaction(self):
        try:
            self.logger.debug("Creating transaction from input")
            transaction = self.create_transaction_from_input()
            shares = transaction.shares
            if shares < 0.0001:  # Minimum tradable amount
                raise ValueError("Amount results in less than 0.0001 shares")
            self.logger.debug(f"Created transaction: {transaction}")
            self.logger.debug("Adding transaction to portfolio")
            self.portfolio.add_transaction(transaction)
            self.logger.debug("Transaction added successfully")
            self.update()
            self.update_callback()
            QMessageBox.information(self, "Success", "Transaction added successfully!")
            self.clear_inputs(self.transaction_entries)
        except ValueError as e:
            self.logger.error(f"Invalid input for transaction: {str(e)}")
            QMessageBox.warning(self, "Error", f"Invalid input: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unexpected error adding transaction: {str(e)}", exc_info=True)
            QMessageBox.critical(self, "Error", f"An unexpected error occurred: {str(e)}")

    def add_cash_flow(self):
        try:
            self.logger.debug("Creating cash flow from input")
            cash_flow = self.create_cash_flow_from_input()
            self.logger.debug(f"Created cash flow: {cash_flow}")
            self.logger.debug("Adding cash flow to portfolio")
            self.portfolio.add_cash_flow(cash_flow)
            self.logger.debug("Cash flow added successfully")
            self.update()
            self.update_callback()
            QMessageBox.information(self, "Success", "Cash flow added successfully!")
            self.clear_inputs(self.cash_flow_entries)
        except ValueError as e:
            self.logger.error(f"Invalid input for cash flow: {str(e)}")
            QMessageBox.warning(self, "Error", f"Invalid input: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unexpected error adding cash flow: {str(e)}", exc_info=True)
            QMessageBox.critical(self, "Error", f"An unexpected error occurred: {str(e)}")

    def create_transaction_from_input(self):
        date = datetime.strptime(self.transaction_entries['date'].text(), "%Y-%m-%d").date()
        symbol = self.transaction_entries['symbol'].text().strip().upper()
        action = self.transaction_entries['action'].currentText()
        amount = float(self.transaction_entries['amount'].text().strip())
        price = float(self.transaction_entries['price'].text().strip())
        fees = float(self.transaction_entries['fees'].text().strip() or 0)

        return create_transaction(date, symbol, action, amount, price, fees)

    def create_cash_flow_from_input(self):
        date = datetime.strptime(self.cash_flow_entries['date'].text(), "%Y-%m-%d").date()
        amount = float(self.cash_flow_entries['amount'].text().strip())
        flow_type = self.cash_flow_entries['flow_type'].currentText()

        return create_cash_flow(date, amount, flow_type)

    def clear_inputs(self, entries):
        for entry in entries.values():
            if isinstance(entry, QLineEdit):
                entry.clear()
            elif isinstance(entry, QComboBox):
                entry.setCurrentIndex(0)

    def update(self):
        self.update_transaction_history()
        self.update_cash_flow_history()

    def update_transaction_history(self):
        self.transaction_table.setRowCount(0)
        for t in self.portfolio.get_transaction_history():
            row = self.transaction_table.rowCount()
            self.transaction_table.insertRow(row)
            self.transaction_table.setItem(row, 0, QTableWidgetItem(t.date.strftime("%Y-%m-%d")))
            self.transaction_table.setItem(row, 1, QTableWidgetItem(t.symbol))
            self.transaction_table.setItem(row, 2, QTableWidgetItem(t.action))
            self.transaction_table.setItem(row, 3, QTableWidgetItem(f"${t.amount:.2f}"))
            self.transaction_table.setItem(row, 4, QTableWidgetItem(f"${t.price:.2f}"))
            self.transaction_table.setItem(row, 5, QTableWidgetItem(f"{t.shares:.4f}"))
            self.transaction_table.setItem(row, 6, QTableWidgetItem(f"${t.fees:.2f}"))

    def update_cash_flow_history(self):
        self.cash_flow_table.setRowCount(0)
        for cf in self.portfolio.get_cash_flow_history():
            row = self.cash_flow_table.rowCount()
            self.cash_flow_table.insertRow(row)
            self.cash_flow_table.setItem(row, 0, QTableWidgetItem(cf.date.strftime("%Y-%m-%d")))
            self.cash_flow_table.setItem(row, 1, QTableWidgetItem(f"${cf.amount:.2f}"))
            self.cash_flow_table.setItem(row, 2, QTableWidgetItem(cf.flow_type))
