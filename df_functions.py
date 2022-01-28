from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructField,StructType,ArrayType
from pyspark.sql.functions import col
from pyspark.sql.functions import array_contains,sum,max,avg,lit,when,expr,concat,concat_ws,date_format,current_date,date_add,date_sub,datediff


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Where filter") \
        .master('local[*]') \
        .getOrCreate()

    df_1 = spark.createDataFrame([('2021-01-15', '2021-02-15',)], ['start_dt', 'end_dt'])
    # df_1.show()
    # df_1.printSchema()

    df_2 = df_1.select(df_1.start_dt.cast('date'),df_1.end_dt.cast('date'))
    df_2.show()
    df_2.printSchema()

#change date format
    df_2.select("start_dt","end_dt",date_format("start_dt",'dd/MM/yyyy').alias("dt_format")).show()

#fetch current date
    df_2.select("start_dt","end_dt",current_date().alias("current_date")).show()

# add days to date
    df_2.select("start_dt","end_dt",date_add("start_dt",2).alias("add_2_days")).show()

#sub days
    df_2.select("start_dt","end_dt",date_add("start_dt",-2).alias("add_2_days")).show()

# datedif
    df_2.select("start_dt", "end_dt", datediff("start_dt", "end_dt").alias("days_diff")).show()

