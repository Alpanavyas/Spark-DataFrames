from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructField,StructType,IntegerType
from pyspark.sql.functions import col,row_number,rank,dense_rank,ntile,lag,avg,explode


if __name__ == '__main__':
    spark = SparkSession.builder\
                .appName('Json-Parquae')\
                .master('local[*]')\
                .getOrCreate()

    jsonDf = spark.read.format("json").load("file:///home/saif/LFS/datasets/users.json")
    # jsonDf.show()
    # jsonDf.printSchema()

    # myschema =  StructType([
    #     StructField('name',
    #                 StructType([
    #                     StructField('fname', StringType(), True),
    #                     StructField('mname', StringType(), True)
    #                 ])),
    #     StructField('company_name', StringType(),True),
    #     StructField('Full_address',
    #                 StructType([
    #                     StructField('address',StringType(),True),
    #                     StructField('city',StringType(),True),
    #                     StructField('country',StringType(),True),
    #                     StructField('state',StringType(),True),
    #                     StructField('zip',IntegerType(),True)
    #                 ])),
    #     StructField('age',IntegerType(),True),
    #     StructField('Phone_Number',
    #                 StructType([
    #                     StructField('phone1',StringType(),True),
    #                     StructField('phone2',StringType(),True),
    #                     StructField('email',StringType(),True),
    #                     StructField('web',StringType(),True)
    #                 ]))
    # ])

    myschema = StructType([StructField('first_name', StringType(), True),
                           StructField('last_name', StringType(), True),
                           StructField('company_name', StringType(), True),
                           StructField('address', StringType(), True),
                           StructField('city', StringType(), True),
                           StructField('county', StringType(), True),
                           StructField('state', StringType(), True),
                           StructField('zip', IntegerType(), True),
                           StructField('age', IntegerType(), True),
                           StructField('phone1', StringType(), True),
                           StructField('phone2', StringType(), True),
                           StructField('email', StringType(), True),
                           StructField('web', StringType(), True)])

    df1 = spark.read.schema(myschema).json("file:///home/saif/LFS/datasets/users.json")
    df1.show(truncate=False)


    readParquet = spark.read.parquet("file:///home/saif/LFS/datasets/Users.parquet")
    readParquet.show(5,truncate=False)
    cnt = df1.union(readParquet).distinct().count()
    print(cnt)


# # corrupt record
#     r1json = spark.read.json("file:///home/saif/LFS/datasets/batters.json")
#     r1json.show(5)

    batterDf = spark.read.format('json')\
                .option('multiline','True')\
                .load("file:///home/saif/LFS/datasets/batters.json")
    batterDf.show(5)
    batterDf.printSchema()


    df2 = batterDf.withColumn("batters", explode("batters.batter"))\
                    .withColumn("topping",explode("topping"))
    df2.printSchema()
    df2.show(truncate=False)

    df3 = df2.select("batters.id","batters.type","id","name","ppu","topping.id","topping.type","type")
    df3.show()

