import logging
import os
from datetime import datetime, timezone


class CustomFormatter(logging.Formatter):
    grey = '\033[92m'
    yellow = '\033[96m'
    red = '\033[93m'
    bold_red = '\033[91m'
    reset = '\x1b[0m'
    format = "%(asctime)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: grey + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def setup_logging(logging_level: str = None):
    raw_db = os.environ.get('DATACOVES__LOADER__DATABASE')
    if raw_db == 'RAW_DEV':
        print('WARNING: Using the RAW_DEV development database. Press any key to continue.')
        input()

    # Create the logs folder
    if not os.path.exists('logs'):
        os.makedirs('logs')
    if logging_level is None:
        logging_level = os.environ.get('LOGGING_LEVEL', 'INFO')

    if logging_level == 'DEBUG':
        level = logging.DEBUG
    elif logging_level == 'INFO':
        level = logging.INFO
    elif logging_level == 'WARNING':
        level = logging.WARNING
    elif logging_level == 'ERROR':
        level = logging.ERROR
    else:
        level = logging.INFO

    now_time_str = datetime.now(timezone.utc).replace(tzinfo=None).strftime('%Y-%m-%dT%H_%M_%S')

    logger = logging.getLogger()
    logger.setLevel(level)

    # logging.basicConfig(level=level,
    #                     format="%(asctime)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)",
    #                     datefmt='%Y-%m-%d %H:%M:%S',
    #                     filename=os.path.join('logs', f'VS_Extractor-{now_time_str}.log'))

    # Adding a file handler
    log_filename = os.path.join('logs', f'VS_Extractor-{now_time_str}.log')
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(level)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"))

    logging.getLogger().addHandler(file_handler)

    # Print log messages to console at the same time.
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(CustomFormatter())
    logging.getLogger().addHandler(console_handler)
