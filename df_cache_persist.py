from pyspark.sql import SparkSession
from pyspark import SparkConf,SparkContext
from urllib.request import urlopen
from pyspark.sql.functions import explode,col
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':
    sparkConf = SparkConf()
    sc = SparkContext(conf=sparkConf)
    spark = SparkSession.builder\
                .appName('Cache-Persist')\
                .master('local[*]')\
                .getOrCreate()

    myDf = spark.read.format("csv")\
                .options(header="true")\
        .option('inferSchema', 'True') \
        .load('file:///home/saif/LFS/datasets/txn1')

    # cat txns >> txn1
    myDf.show()

# # cache
#     cacheDf = myDf.where(col("state") == "California").cache()
#     print(cacheDf.count())
#
#     readingCacheDf = cacheDf.where(col("spendby") == "credit")
#     print(readingCacheDf)

    # it will show at this location: http: // localhost: 4040
    # input("Enter")

    # persistDf = myDf.persist(StorageLevel.DISK_ONLY)

    persistDf = myDf.persist(StorageLevel.MEMORY_AND_DISK)
    persistDf.count()
    input("Enter")

    # WHENEVER WE USE PERSIST, WE NEED TO USE UNPERSIST -> persistDf.unpersist

