from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructField,StructType,ArrayType
from pyspark.sql.functions import col
from pyspark.sql.functions import array_contains,sum,max,avg


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Where filter") \
        .master('local[*]') \
        .getOrCreate()
    orderingData = [("Saif", "Sales", "HYD", 90000, 34, 10000),
                    ("Ram", "Sales", "HYD", 86000, 56, 20000),
                    ("Aniket", "Sales", "MUM", 81000, 30, 23000),
                    ("Saima", "Finance", "MUM", 90000, 24, 23000),
                    ("Sufiyan", "Finance", "MUM", 99000, 40, 24000),
                    ("Alim", "Finance", "HYD", 83000, 36, 19000),
                    ("Mitali", "Finance", "HYD", 79000, 53, 15000),
                    ("Neha", "Marketing", "MUM", 80000, 25, 18000),
                    ("Kajal", "Marketing", "HYD", 91000, 50, 21000)]

    columns = ["employee_name","department","state","salary","age","bonus"]
    df = spark.createDataFrame(orderingData,columns)
    df.show()

    df.sort("department","state").show(truncate=False)
    df.orderBy("department", "state").show(truncate=False)

    # desc
    df.sort(col("department"),col("state").desc()).show(truncate=False)


    df.groupBy("department").sum("salary").show(truncate=False)

# Alias
    df6 = df.groupBy("department").agg(sum("salary").alias("SumOfSal"))
    df6.show(truncate=False)

    df.groupBy("department","state")\
        .sum("salary","bonus").show(truncate=False)

    df.groupBy('department')\
        .agg(sum("salary").alias("sum_salary"),\
             avg("salary").alias("avg_salary"),\
             sum("bonus").alias("sum_bonus"),\
             max("bonus").alias("max_bonus")\
             ).show(truncate=False)

    # .where(col("sum_bonus") >= 50000)\