import os
from pathlib import Path
import json
import boto3
from datetime import datetime as dt
from functools import wraps
import modules.data_manager as dm
from logging import getLogger, StreamHandler, DEBUG

logger = getLogger(__name__)
handler = StreamHandler()
handler.setLevel(DEBUG)
logger.setLevel(DEBUG)
logger.addHandler(handler)
logger.propagate = False


def add_exception(func):
    @wraps(func)
    def add_exception(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.debug(f"Error occured in function {func.__name__} : {e}")
            return None

    return add_exception


def calculate_time(func):
    @wraps(func)
    def calculate_time(*args, **kwargs):
        st = dt.now()
        result_func = func(*args, **kwargs)
        et = (dt.now() - st).seconds
        logger.debug(f"{func.__name__} takes {et} seconds")
        return result_func

    return calculate_time


@add_exception
@calculate_time
def lambda_handler(event, context):
    pass


if __name__ == "__main__":
    event = {}
    context = {}
    lambda_handler(event, context)
