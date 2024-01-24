import os.path
import shutil

from spark_session import create_spark_session
from data_loading import load_json_data
from data_saving import query_data, save_data_as_table
import unittest


class RetailSalesAnalysisTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Spark Session 생성 및 기타 클래스 레벨 설정
        cls.spark = create_spark_session("Retail Sales Analysis")
        cls.data_path = "../data/sales_data.json"
        cls.table_name = "sales_data"

    def setUp(self):
        # 각 테스트 전 설정: 데이터 적제
        self.sales_df = load_json_data(self.spark, self.data_path)
        save_data_as_table(self.sales_df, self.table_name)

    @classmethod
    def tearDownClass(cls):
        # 모든 테스트 후 : Spark Session 종료
        cls.spark.stop()

        warehouse = "./spark-warehouse"

        if os.path.isdir(warehouse):
            shutil.rmtree(warehouse)
            print(f"디렉토리 삭제 완료 : {warehouse}")
        else:
            shutil.rmtree(f"{warehouse} 해당 디렉토리는 존재하지 않습니다.")


if __name__ == "__main__":
    unittest.main()
