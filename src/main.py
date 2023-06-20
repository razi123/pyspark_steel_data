# python modules
import os

# local user defined modules
import config.util as conf
from src import challenge_1 as src_ch1


if __name__ == "__main__":
    # spark cluster
    spark = conf.create_spark_session()

    # dataset path
    path_dataset = "C:/Users/raziuddin.khazi/PycharmProjects/pyspark_steel_data/datasets/"

    dataset_cars = os.path.join(path_dataset, "cars.csv")
    dataset_sales = os.path.join(path_dataset, "sales.csv")
    dataset_salespersons = os.path.join(path_dataset, "salespersons.csv")

    # read the datasets
    df_ch1_cars = spark.read.format('csv').option('header', 'True').load(dataset_cars)
    df_ch1_sales = spark.read.format('csv').option('header', 'True').load(dataset_sales)
    df_ch1_salesmans = spark.read.format('csv').option('header', 'True').load(dataset_salespersons)

    # answers 1 : Details of all cars sold in 2022
    df_answer_1 = src_ch1.question1(df_ch1_cars, df_ch1_sales)



