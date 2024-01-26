import logging
import sys
import textwrap
from typing import Callable
from datetime import datetime
from abc import ABCMeta, abstractmethod

_logger = logging.getLogger()
_logger.setLevel(logging.INFO)
# 로그 핸들러 추가 :  로그 메시지 표준 출력
_logger.addHandler(logging.StreamHandler(sys.stdout))


def time_elapsed(func: Callable) -> Callable:
    def wrapper(self, *args, **kwargs):
        start_date = datetime.now()
        _logger.info(f"started at : {start_date}")

        func(self, *args, **kwargs)

        end_date= datetime.now()
        _logger.info(textwrap.dedent(f"""
        ended at : {end_date}
        total elapsed : {(start_date - end_date).total_seconds()} sec
        """))

        return wrapper


class ETL(metaclass=ABCMeta):

    def __int__(self):
        self.spark = create_session()