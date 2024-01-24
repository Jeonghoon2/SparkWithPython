import sys
import textwrap
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as F

import os


def create_session(app_name) -> SparkSession:
    spark = (SparkSession.builder
             .appName(app_name)
             .master("local")
             .config("spark.driver.bindAddress", "127.0.0.1")
             .enableHiveSupport()
             .getOrCreate())
    return spark


def table_exists(spark: SparkSession, table_name: str) -> bool:
    tables_list = spark.sql("SHOW TABLES")
    result = tables_list.filter(tables_list.tableName == table_name).count() > 0
    return result


def read(spark: SparkSession, path: str, dt: str, hh: str) -> DataFrame:
    schema = StructType([
        StructField("traceId", StringType(), True),
        StructField("clientIp", StringType(), True),
        StructField("time", StringType(), True),
        StructField("path", StringType(), True),
        StructField("method", StringType(), True),
        StructField("request", StringType(), True),
        StructField("response", StringType(), True),
        StructField("statusCode", StringType(), True),
        StructField("elapsedTimeMillis", IntegerType(), True)
    ])

    df = spark.read.json(path, schema=schema)
    df = df.withColumn("etl_cre_dt", lit(dt))
    df = df.withColumn("etl_cre_hh", lit(hh))

    return df


def load(spark: SparkSession) -> DataFrame:
    load_df = spark.sql(textwrap.dedent("""
        SELECT *
        FROM log_ingest_raw
    """))

    return load_df


def merge_write(new_df: DataFrame, load_df: DataFrame, table_name: str):
    new_df = new_df.withColumn("etl_cre_dtm", F.lit(datetime.now()))

    new_df.printSchema()

    # 기존 데이터와 현재 데이터 합치기
    union_df = new_df.union(load_df)

    # row_no 컬럼 생성 후 같은 traceId끼리 PartitionBy 정렬 기준은 etl_cre_dt 기준
    union_df = union_df.withColumn("row_no", F.row_number().over(
        Window.partitionBy("traceId").orderBy(F.col('etl_cre_dtm').desc())))

    print("행과 컬럼을 제거 전")
    union_df.show()

    # 첫번째 행을 제외하고 제거 (최신의 데이터만을 남기기 위해)
    union_df = union_df.filter(F.col("row_no") == F.lit(1)).drop("row_no", "etl_cre_dtm")
    print("행과 컬럼을 제거 후")
    union_df.show()

    # Error 방지 Read한 곳에는 wrtie 할 수 없다는 뜻
    union_df = union_df.localCheckpoint()

    union_df.write.mode("overwrite").insertInto(table_name)


def write(df: DataFrame, table_name: str):
    df.write.mode("overwrite").saveAsTable(table_name)


if __name__ == '__main__':
    dt, hh = sys.argv[1], sys.argv[2]
    try:
        # Spark Session 생성
        spark = create_session("log_data_ingest")

        # Json 데이터 읽기
        new_df = read(spark, f"/data/ingestion/{dt}/{hh}/log-*.json", dt, hh)

        table_name = "log_ingest"

        # 테이블 확인 여부
        if spark.catalog.tableExists(table_name):
            # Json 데이터 읽기
            new_df = read(spark, dt, hh)

            # 기존 Spark 데이터 로드
            load_df = load(spark)

            # 기존 데이터 및 병합
            merge_write(new_df, load_df, table_name)
        else:
            # 최초 테이블에 데이터 적제 할 경우
            write(new_df, table_name)

    except Exception as e:
        os.system(f"echo {e}")
