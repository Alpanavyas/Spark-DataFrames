from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructField,StructType,ArrayType
from pyspark.sql.functions import col
from pyspark.sql.functions import array_contains,sum,max,avg,lit,when,expr,concat,concat_ws


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Where filter") \
        .master('local[*]') \
        .getOrCreate()

    data = [("Saif", "M", "Shaikh", "2018", "M", 3000),
            ("Ram", "S", "Shirali", "2010", "M", 4000),
            ("Mitali", "S", "Kashiv", "2010", "M", 4000),
            ("Anup", "B", "Garje", "2005", "F", 4000),
            ("Sagar", "S", "Shinde", "2010", "", -1)]
    columns = ["fname", "mname", "lname", "dob_year", "gender", "salary"]
    df = spark.createDataFrame(data=data, schema=columns)

    # concat_ws ->concat the data
    df3 = df.select(concat_ws('_',col("fname"),col("mname"),col("lname"))
                   .alias("FullName"),"dob_year","gender","salary")
    df3.show(truncate=False)


    df2 =df.select(concat("fname",lit(","),"mname",lit(","),"lname")
                   .alias("FullName"),"dob_year","gender","salary")
    df2.show(truncate=False)
