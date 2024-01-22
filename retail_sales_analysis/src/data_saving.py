def save_data_as_temp_table(df, table_name):
    #  데이터프레임에 대한 임시 테이블을 생성
    df.createOrReplaceTempView(table_name)


def query_data(spark, query):
    # Query 실행
    return spark.sql(query)
