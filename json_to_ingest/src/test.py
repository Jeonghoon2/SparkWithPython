import os
import shutil
import unittest
from datetime import datetime

from etl.ETLImpl import ETLImpl
import pyspark.sql.functions as F


class UpsertTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = ETLImpl.create_session("ingest_test", local=True)

    def test_ingest(self):
        etl = ETLImpl()
        etl.base_dt = datetime(2024, 1, 24)
        etl.args['p_hh'] = '18'

        base_sample = "../data/SampleDataA.json"
        init_df = etl.read(base_sample)

        init_df = init_df.withColumn("etl_cre_dtm", F.current_timestamp())
        etl.write(init_df, "log_ingest", ['traceId'], 'etl_cre_dtm')

        upsert_data = "../data/SampleDataA-2.json"

        df = etl.read(upsert_data)
        df = etl.process(df)
        etl.write(df, "log_ingest", ['traceId'], 'etl_cre_dtm')

        actual_df = self.spark.sql("SELECT * FROM log_ingest")

        actual_df.show()
        actual = [r[0] for r in actual_df.select("statusCode").collect()]
        self.assertListEqual(actual, ["200"])

    @classmethod
    def tearDownClass(cls):
        metastore = "./metastore_db"
        warehouse = "./spark-warehouse"
        log = "./derby.log"

        if os.path.isdir(warehouse):
            shutil.rmtree(metastore)
            shutil.rmtree(warehouse)
            os.remove(log)
            print(f"디렉토리 삭제 완료 : {warehouse}")
            print(f"디렉토리 삭제 완료 : {warehouse}")
        else:
            shutil.rmtree(f"{warehouse} 해당 디렉토리는 존재하지 않습니다.")
