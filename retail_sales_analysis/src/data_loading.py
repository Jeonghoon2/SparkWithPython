def load_json_data(spark, file_path):
    df = spark.read.json(file_path)
    return df