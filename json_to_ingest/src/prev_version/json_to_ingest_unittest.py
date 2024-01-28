import shutil
from datetime import datetime

import pyspark.sql.functions as F
from json_data_ingest import write, load, read, create_session, merge_write, table_exists
import unittest
import os


class table_exists_test(unittest.TestCase):

    def setUp(self):
        self.spark = create_session("table_test")
        self.table_name = 'log_ingest'

    def test_none_tale(self):
        result = table_exists(self.spark, self.table_name)

        self.assertEquals(result, False)

        self.spark.stop()

    def test_table_correct(self):
        df = read(self.spark, "../../data/SampleDataA.json", "2024-01-24", "18")
        write(df, self.table_name)

        df.show()
        result = table_exists(self.spark, self.table_name)
        self.assertEquals(result, True)

        self.spark.stop()


class ingest_test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = create_session("ingest_test")

    def test_deduplication(self):

        table_name = "log_ingest"
        data_samples = [
            "../data/SampleDataB.json",
            "../data/SampleDataC.json"
        ]

        for idx, data_sample in enumerate(data_samples):
            print(f"{idx + 1}번째 샘플 ")
            if table_exists(self.spark, table_name):
                print(f"Find {table_name} Table")

                query1 = f"SELECT * FROM {table_name}"
                load_df = self.spark.sql(query1).withColumn("etl_cre_dtm", F.lit(datetime.now()))

                # 스키마 검증
                load_df.printSchema()

                new_df = read(self.spark, data_sample, "2024-01-24", "18")

                merge_write(new_df, load_df, table_name)

                query1 = f"SELECT * FROM {table_name}"
                result_df = self.spark.sql(query1)

                result_df.show()
                self.assertEqual(result_df.count(), 5)

            else:
                print(f"Not Found {table_name} Table")
                df = read(self.spark, data_sample, "2024-01-24", "18")
                write(df, table_name)

                query1 = f"SELECT * FROM {table_name}"
                self.spark.sql(query1).show()

        self.spark.stop()

    @classmethod
    def tearDownClass(cls):
        metastore = "./metastore_db"
        warehouse = "./spark-warehouse"

        if os.path.isdir(warehouse):
            shutil.rmtree(metastore)
            shutil.rmtree(warehouse)
            print(f"디렉토리 삭제 완료 : {warehouse}")
        else:
            shutil.rmtree(f"{warehouse} 해당 디렉토리는 존재하지 않습니다.")


if __name__ == '__main__':
    unittest.main()
