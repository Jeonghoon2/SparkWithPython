from typing import Optional, List
from pyspark.sql import SparkSession, DataFrame, Window
from datetime import datetime

from json_to_ingest.src.etl.ETL import ETL


class ETLImpl(ETL):

    # Spark Session 생성
    def create_session(app_name: Optional[str] = None, local: bool = False) -> SparkSession:
        spark = SparkSession.getActiveSession()

        if spark is not None:
            return spark

        if app_name is None:
            app_name = f"spark-app-{int(datetime.now().timestamp())}"

        builder = SparkSession.builder

        # 로컬 환경일 경우
        if local:
            builder = (builder
                       .master(local)
                       .config("spark.driver.bindAddress", "127.0.0.1")
                       )

        spark = (builder
                 .appName(app_name)
                 .enableHiveSupport()
                 .getOrCreate()
                 )
        return spark


