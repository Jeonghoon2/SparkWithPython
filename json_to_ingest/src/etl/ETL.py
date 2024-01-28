import argparse
import logging
import sys
import textwrap
from typing import Callable, NoReturn
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

        end_date = datetime.now()
        _logger.info(textwrap.dedent(f"""
        ended at : {end_date}
        total elapsed : {(start_date - end_date).total_seconds()} sec
        """))

        return wrapper


class ETL(metaclass=ABCMeta):

    @abstractmethod
    def create_session(self):
        ...

    def __init__(self):
        self.spark = self.create_session()
        self.parser: argparse.ArgumentParser = argparse.ArgumentParser()
        self.parser.add_argument('--base_dt', type=lambda s: datetime.strptime(s, '%Y-%m-%d'), required=True)
        self.args = {}
        self.base_dt: datetime = None
        self.prev_dt: datetime = None
        self.next_dt: datetime = None

    def args_define(self) -> NoReturn:
        ...

    @abstractmethod
    def read(self):
        ...

    @abstractmethod
    def process(self):
        ...

    @abstractmethod
    def write(self):
        ...

    @time_elapsed
    def run(self):
        self.args_define()
        self.args = self.parser.parse_args().__dict__
        self.base_dt = self.args['base_dt']
        self.prev_dt = self.base_dt - timedelta(days=1)
        self.next_dt = self.base_dt + timedelta(days=1)

        res = self.read(f"/data/ingestion/{self.base_dt}/{self.args['p_dt']}/log-*.json")
        res = self.process(res)
        self.write(res, "log_ingest", ['traceId'], 'etl_cre_dtm')
