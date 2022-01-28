from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructField,StructType,ArrayType
from pyspark.sql.functions import col
from pyspark.sql.functions import array_contains,sum,max,avg,lit,when,expr


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Where filter") \
        .master('local[*]') \
        .getOrCreate()

    data = [("Saif", "", "Shaikh", "36636", "M", 60000),
            ("Ram", "Shirali", "", "40288", "M", 70000),
            ("Aniket", "", "Mishra", "42114", "", 400000),
            ("Mitali", "Sahil", "Kashiv", "39192", "F", 500000),
            ("Nahid", "Alim", "Shaikh", "", "F", 0)]

    columns = ['fname','mname','lname','num1','gender','sal']
    df = spark.createDataFrame(data=data, schema=columns)
    # df.show()

    df2 = df.withColumn("new_gender",
                        when(col('gender') == "M","Male")
                        .when(col('gender') == "F","Female")
                        .otherwise('unknow'))
    df2.show(truncate=False)

    exprDf1 = df.withColumn("new_gender",
                                expr(
                                    "case when gender= 'M' then 'Male'"
                                            "when gender = 'F' then 'Female'"
                                            "else 'unknown' end"))
    exprDf1.show()


