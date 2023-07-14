import os
from configparser import ConfigParser
import logging as logger


def get_path():
    return os.path.realpath(os.path.join(os.path.dirname(__file__), '../..'))


def get_config():
    _config = ConfigParser()
    path = os.path.join(get_path(), "src", "config.ini")
    _config.read(path)
    return _config


def get_logger():
    logger.basicConfig(level=logger.INFO)
    return logger
