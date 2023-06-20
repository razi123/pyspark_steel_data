from pyspark.sql import DataFrame


def equals(df1: DataFrame, df2: DataFrame):
    """

    :param df1:
    :param df2:
    :return:
    """

    if df1.exceptAll(df2).count() == df2.exceptAll(df1).count():
        print("The DataFrames have equal rows")
    else:
        print("The DataFrames have unequal rows")

    if df1.schema == df2.schema:
        print("The Dataframes have same schema and columns")
    else:
        print("Schema dont match")
        print("df1 = ", df1)
        print("df2 = ", df2)