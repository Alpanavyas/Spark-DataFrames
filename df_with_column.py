from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructField,StructType,ArrayType
from pyspark.sql.functions import col
from pyspark.sql.functions import array_contains,sum,max,avg,lit


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Where filter") \
        .master('local[*]') \
        .getOrCreate()
    data = [('Saif', '', 'Shaikh', '1991-04-01', 'M', 3000),
            ('Ram', 'Sachin', '', '2000-05-19', 'M', 4000),
            ('Aniket', '', 'Mishra', '1978-09-05', 'M', 4000),
            ('Mitali', 'Sahil', 'Kashiv', '1967-12-01', 'F', 4000),
            ('Nahid', 'Alim', 'Shaikh', '1980-02-17', 'F', -1)]

    columns = ["firstname","Middlename","Lname","dob","gender","salary"]
    df = spark.createDataFrame(data=data,schema=columns)
    df.show()

    df2 = df.withColumn("salary1",col("salary").cast("Integer"))
    df2.printSchema()

    df3 = df.withColumn("Inc_Salary",col("salary")*100).drop('salary')
    df3.show(5)

    # lit -> give dummy value
    df4 = df.withColumn("Country",lit("IND"))\
        .withColumn("State",lit("MH"))
    df4.show()

# rename the column names
    df.withColumnRenamed("gender","NewGender").show(truncate=False)

# created a view
    sqlT = df.createOrReplaceTempView('Prasad')
    spark.sql('select * from Prasad').show()