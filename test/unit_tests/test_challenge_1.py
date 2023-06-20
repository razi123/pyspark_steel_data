from pyspark.sql import DataFrame
import pandas as pd
import datetime as dt

import src.challenge_1 as test_src_ch1
import config.util as conf
from test.test_functions import test_function as tf

spark = conf.create_spark_session()


def test_question1() -> DataFrame:
    """
    test data for question1 function
    :return:
    """

    df_ch1_cars = spark.createDataFrame(
        pd.DataFrame({
        'car_id' : [1, 2, 3, 4, 5],
        'make' : ['Honda', 'Toyoto', 'Ford', 'BMW', 'Audi'],
        'type': ['Civic', 'Corolla', 'Explorer', 'X5', 'A4'],
        'style': ['Sedan', 'Hatchback', 'SUV', 'SUV', 'Sedan'],
        'cost_$': [30000, 25000, 40000, 36000, 55000]
         })
    )

    df_ch1_sales = spark.createDataFrame(
        pd.DataFrame({
            'sale_id': [1, 2, 3, 4, 5],
            'car_id': [1, 3, 2, 5, 8],
            'salesman_id': [1, 3, 2, 4, 1],
            'purchase_date': [dt.date(2022, 1, 1), dt.date(2021, 1, 1), dt.date(2022, 7, 1),
                          dt.date(2023, 7, 1), dt.date(2020, 4, 18)
                          ],
        }))

    df_output = test_src_ch1.question1(df_ch1_cars, df_ch1_sales)

    df_expected = spark.createDataFrame(
        pd.DataFrame({
        'car_id': [1, 3],
        'make': ['Honda', 'Ford'],
        'type': ['Civic', 'Explorer'],
        'style': ['Sedan', 'SUV'],
        'cost_$': [30000, 40000],
        'purchase_date': [dt.date(2022, 1, 1), dt.date(2022, 7, 1)],
    })
    )

    tf.equals(df_expected, df_output)






