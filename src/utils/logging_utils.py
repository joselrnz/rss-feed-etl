import logging

def setup_logging():
    """Configures basic logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
            # Add FileHandler for logging to a file if needed
            # logging.FileHandler("logs/app.log")
        ]
    )

# Call setup immediately so logging is configured when the module is imported
setup_logging()
logger = logging.getLogger(__name__)