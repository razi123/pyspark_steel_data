from pyspark.sql import functions as f
from pyspark.sql import DataFrame


def question1(df_ch1_cars:DataFrame, df_ch1_sales:DataFrame)-> DataFrame:
    """
    Question1: What are the details of all cars purchased in the year 2022?
    :param spark:
    :param df_ch1_cars: dataframe with cars information, eg: model, type, style, cost etc
    :param df_ch1_sales: dataframe with car sales information, eg: sales_id, purchase_id
    :return:
    a dataframe with cars information that were purchased in the year 2022
    """

    df_ch1_sales = df_ch1_sales.withColumnRenamed("car_id", "sales_car_id")
    df_cars_detail = df_ch1_cars.join(df_ch1_sales, df_ch1_cars.car_id == df_ch1_sales.sales_car_id, 'inner').\
            where(f.year(df_ch1_sales.purchase_date) == 2022).select( "car_id",
                                                                       "make",
                                                                        "type",
                                                                        "style",
                                                                        "cost_$",
                                                                        "purchase_date"
                                                                      )

    return df_cars_detail

