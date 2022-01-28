from pyspark.sql import SparkSession

if __name__ == '__main__':

    # * -> all
    spark = SparkSession.builder.appName('creatingDataFrame').master("local[*]").getOrCreate()

    df = spark.read.format('csv')\
        .option('header','true')\
        .option('inferSchema','True')\
        .load('file:///home/saif/LFS/datasets/orders.txt')
    # truncated the file
    df.show(5,truncate=False)
    # To see the schema
    df.printSchema()