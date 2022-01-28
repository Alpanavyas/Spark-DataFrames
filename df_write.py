from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder\
        .appName('Different Write Modes')\
        .master('local[*]')\
        .getOrCreate()

    df = spark.read.format('csv')\
        .options(header = 'True',inferSchema='True',delimeter=',')\
        .load('file:///home/saif/LFS/datasets/movies.csv')

    df1 = spark.read\
        .options(header = 'True',inferSchema='True',delimeter=',')\
        .csv('file:///home/saif/LFS/datasets/movies.csv')

    # filDf = df.filter(col('movieId') == 10)
    # filDf.show()

    filDf = df.where("movieId == 10")
    filDf.show()

# Save modes
#     default -> error
#     .mode('ignore')
#     .mode('append')
#     .mode('overwrite')

    #write:
    filDf.write.format('csv')\
        .mode('ignore')\
        .save('hdfs://localhost:9000/user/saif/HFS/Output/c8_saveMode')
    print("****************Success********************")


    # df.show(5, truncate=False)
    # df.printSchema()

    # df1.show(5, truncate=False)
    # df1.printSchema()