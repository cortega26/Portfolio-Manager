"""
Main entry point for the Portfolio Manager application.

This script initializes and runs the PySide6-based GUI for the Portfolio Manager.
It sets up logging and handles any exceptions that occur during startup.
"""

import sys
import logging
from PySide6.QtWidgets import QApplication
from gui.portfolio_manager_gui_pyside import PortfolioManagerGUI

def setup_logging() -> None:
    """Set up logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        filename='portfolio_manager.log'
    )

def main() -> None:
    """
    Initialize and run the Portfolio Manager application.
    
    This function sets up logging, creates the main application window,
    and starts the event loop. Any unhandled exceptions are logged before
    the application exits.
    """
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Starting Portfolio Manager application")
        app = QApplication(sys.argv)
        window = PortfolioManagerGUI()
        window.show()
        sys.exit(app.exec())
    except Exception as e:
        logger.exception(f"An error occurred while starting the application: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()