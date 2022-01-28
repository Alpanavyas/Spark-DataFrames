from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructField,StructType,ArrayType
from pyspark.sql.functions import col
from pyspark.sql.functions import array_contains,sum,max,avg,lit,when,expr,concat,concat_ws,date_format,current_date,date_add,date_sub,datediff
from pyspark.sql.functions import approx_count_distinct,collect_list,collect_set,countDistinct,count,first,last,sumDistinct

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Where filter") \
        .master('local[*]') \
        .getOrCreate()

    simpleData = [("Saif", "Sales", 3000),
                  ("Ram", "Sales", 4600),
                  ("Aniket", "Sales", 4100),
                  ("Mitali", "Finance", 3000),
                  ("Saif", "Sales", 3000),
                  ("Sandeep", "Finance", 3300),
                  ("John", "Finance", 3900),
                  ("Jeff", "Marketing", 3000),
                  ("Sagar", "Marketing", 2000),
                  ("Swaroop", "Sales", 4100)]
    schema = ["employee_name", "department", "salary"]
    agg_df = spark.createDataFrame(data=simpleData, schema=schema)
    agg_df.printSchema()
    agg_df.show(truncate=False)

# unique values
    approxDistinctCount = agg_df.select(approx_count_distinct("salary")).show(truncate=False)
# get the repeated records
    agg_df.select(collect_list("salary")).show(truncate=False)
# get the unique records
    agg_df.select(collect_set("salary")).show(truncate=False)
# count distinct
    agg_df.select(countDistinct("department","salary")).show(truncate=False)
# first
    agg_df.select(first("salary")).show(truncate=False)

    agg_df.select(sumDistinct("salary")).show(truncate=False)
