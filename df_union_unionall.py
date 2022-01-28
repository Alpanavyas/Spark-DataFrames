
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructField,StructType,ArrayType
from pyspark.sql.functions import col
from pyspark.sql.functions import array_contains,sum,max,avg,lit


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Where filter") \
        .master('local[*]') \
        .getOrCreate()

    simpleData1 = [("Saif", "Sales", "MUM", 90000, 34, 10000),
                   ("Aniket", "Sales", "MUM", 86000, 56, 20000),
                   ("Ram", "Sales", "PUN", 81000, 30, 23000),
                   ("Mitali", "Finance", "PUN", 90000, 24, 23000)]

    simpleData2 = [("Saif", "Sales", "MUM", 90000, 34, 10000),
                   ("Mitali", "Finance", "PUN", 90000, 24, 23000),
                   ("Sufiyan", "Finance", "MUM", 79000, 53, 15000),
                   ("Alim", "Marketing", "PUN", 80000, 25, 18000),
                   ("Amit", "Marketing", "MUM", 91000, 50, 21000)]
    columns = ["name", "dept", "state", "salary", "age", "bonus"]
    df = spark.createDataFrame(data=simpleData1, schema=columns)
    df.show()
    df2 = spark.createDataFrame(data=simpleData2, schema=columns)
    df2.show()

    df2.unionAll(df).show()
    df2.union(df).show()

    # drop duplicates and distinct
