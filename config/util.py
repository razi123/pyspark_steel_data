from pyspark.sql import SparkSession


def create_spark_session():
    print("Creating spark session")
    spark = SparkSession.builder.\
        master("local[*]").\
        appName("Steel_Data_SQL").\
        config("spark.executor.instances", "4").\
        getOrCreate()

    print("spark session created")
    return spark
