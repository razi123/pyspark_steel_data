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

    df_answer_1 = src_ch1.question1(df_ch1_cars, df_ch1_sales)
    df_answer_2 = src_ch1.question2(df_ch1_sales, df_ch1_salesmans)
    df_answer_3 = src_ch1.question3(df_ch1_cars, df_ch1_sales, df_ch1_salesmans)
    df_answer_4 = src_ch1.question4(df_ch1_cars, df_ch1_sales, df_ch1_salesmans)
    df_answer_5 = src_ch1.question5(df_ch1_cars, df_ch1_sales)
    df_answer_6 = src_ch1.question6(df_ch1_cars, df_ch1_sales, df_ch1_salesmans)
    df_answer_7 = src_ch1.question7(df_ch1_cars, df_ch1_sales)
    df_answer_8 = src_ch1.question8(df_ch1_cars, df_ch1_sales)
    df_answer_9 = src_ch1.question9(df_ch1_sales, df_ch1_salesmans)
    df_answer_10 = src_ch1.question10(df_ch1_cars, df_ch1_sales, df_ch1_salesmans)

    df_answer_1.show()
    df_answer_2.show()
    df_answer_3.show()
    df_answer_4.show()
    df_answer_5.show()

    df_answer_6.show()
    df_answer_7.show()
    df_answer_8.show()
    df_answer_9.show()
    df_answer_10.show()













