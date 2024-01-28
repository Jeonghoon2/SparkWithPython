from datetime import datetime
from typing import NoReturn, Dict, Any, Optional, List
import textwrap
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession, DataFrame, Window

from json_to_ingest.src.etl.ETL import ETL


class ETLImpl(ETL):



    def args_define(self) -> NoReturn:
        self.parser.add_argument("--p_hh", type=str, required=True)

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
                       .master("local")
                       .config("spark.driver.bindAddress", "127.0.0.1")
                       )

        spark = (builder
                 .appName(app_name)
                 .enableHiveSupport()
                 .getOrCreate()
                 )
        return spark

    def read(self, path: str):
        schema = T.StructType([
            T.StructField("traceId", T.StringType(), True),
            T.StructField("clientIp", T.StringType(), True),
            T.StructField("time", T.StringType(), True),
            T.StructField("path", T.StringType(), True),
            T.StructField("method", T.StringType(), True),
            T.StructField("request", T.StringType(), True),
            T.StructField("response", T.StringType(), True),
            T.StructField("statusCode", T.StringType(), True),
            T.StructField("elapsedTimeMillis", T.IntegerType(), True)
        ])

        df = (self.spark
              .read
              .json(path, schema=schema)
              )

        return df

    def process(self, args: Dict[str, Any] | DataFrame) -> Dict[str, Any] | DataFrame:
        df: DataFrame = args
        df = (df
              .withColumn("p_dt", F.lit(self.base_dt.strftime("%Y-%m-%d")))
              .withColumn("p_hh", F.lit(self.args['p_hh']))
              .withColumn("etl_cre_dtm", F.current_timestamp())  # 현재 ETL 시간
              )

        return df

    def write(self,
              df: DataFrame,  # res
              dst_table: str,  # log_ingest
              cols: List[str],  # ['traceId']
              order_col: str):  # 'etl_cre_dtm'

        spark = self.create_session()

        if spark._jsparkSession.catalog().tableExists(dst_table):
            origin_df = spark.sql(textwrap.dedent(f"""
                SELECT *
                  FROM {dst_table}
            """))

            union_df = origin_df.unionAll(df.select(*origin_df.columns))
            window = Window.partitionBy(*cols).orderBy(F.col(order_col).desc())

            union_df = union_df.withColumn("row_no", F.row_number().over(window))
            union_df = union_df.filter(F.col("row_no") == F.lit(1)).drop("row_no")
            union_df = union_df.localCheckpoint()
            union_df.write.mode('overwrite').insertInto(dst_table)
        else:
            df.write.mode("overwrite").saveAsTable(dst_table)