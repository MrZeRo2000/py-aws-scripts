import logging
import os
import datetime
import re

loggers = {}


def get_logger(logger_name: str) -> logging.Logger:
    if logger_name in loggers:
        return loggers.get(logger_name)
    else:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)

        # create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # create console handler and set level to debug
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)

        logger.addHandler(ch)

        log_path = os.path.join(os.path.dirname(__file__), "../log/")
        if not os.path.exists(log_path):
            os.makedirs(log_path)

        current_date = datetime.datetime.now().strftime("%Y_%m_%d_%H")
        log_file_name = f"{log_path}log_{current_date}.log"

        fh = logging.FileHandler(log_file_name)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)

        logger.addHandler(fh)

        loggers[logger_name] = logger

        return logger


def mask_params_string(params_string: str) -> str:
    return re.sub(r"(=\S+)", r"=****", params_string)


def mask_string(api_key: str, limit: int = 4) -> str:
    return '*' * (len(api_key) - limit) + api_key[-limit:]
