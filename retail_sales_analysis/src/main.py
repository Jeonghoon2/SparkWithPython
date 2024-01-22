from spark_session import create_spark_session
from data_loading import load_json_data
from data_saving import save_data_as_temp_table, query_data

if __name__ == "__main__":
    spark = create_spark_session("Retail Sales Analysis")

    data_path = "../data/sales_data.json"  # JSON 파일 경로
    table_name = "sales_data"  # 테이블 이름

    # 데이터 로드
    sales_df = load_json_data(spark, data_path)

    # 데이터 적재
    save_data_as_temp_table(sales_df, table_name)

    # 데이터 조회
    query_result = query_data(spark, f"SELECT * FROM {table_name}")
    query_result.show()

    spark.stop()
