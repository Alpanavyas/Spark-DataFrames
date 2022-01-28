# spark-submit --master yarn --deploy-mode client --driver-memory 1g --executor-memory 2g --num-executors 1 --executor-cores 4 /home/saif/PycharmProjects/cohort_c8/df_xml.py


""" Read and Write XML File """
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from decimal import Decimal
appName = "Python Example - PySpark Read XML"
master = "local"

# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .config('spark.jars','/home/saif/LFS/jars/spark-xml_2.12-0.5.0.jar')\
    .getOrCreate()

schema = StructType([
    StructField('_id', IntegerType(), False),
    StructField('rid', IntegerType(), False),
    StructField('name', StringType(), False)
])

df = spark.read.format("com.databricks.spark.xml")\
    .options(rootTag='catalog', rowTag='book') \
    .load("file:///home/saif/LFS/datasets/book_details.xml")
df.show()

# %%
df.write.format("com.databricks.spark.xml")\
    .options(rootTag='catalog', rowTag='book')\
    .mode("overwrite").\
    save('file:///home/saif/LFS/datasets/book_details')



















# # from pyspark import SparkConf, SparkContext
# from pyspark.sql import SparkSession
#
#
# if __name__ == '__main__':
#     # sparkconf = SparkConf()
#     # sc = SparkContext(conf=sparkconf)
#     spark = SparkSession.builder \
#         .appName("XML file") \
#         .master('local[*]') \
#         .config('spark.jars', 'file:///home/saif/LFS/jars/spark-xml_2.12-0.9.0.jar') \
#         .getOrCreate()
#
#     df = spark.read\
#         .format("com.databricks.spark.xml") \
#         .option("rootTag", "catalog") \
#         .option("rowTag", "book") \
#         .load('file:///home/saif/LFS/datasets/book_details')
#     df.show(4, truncate=False)
#     df.printSchema()
#
#     df.select("rid", "name").write.\
#         format("com.databricks.spark.xml")\
#         .option("rootTag", "data")\
#         .option("rowTag","record").save('file:///home/saif/LFS/datasets/test.xml')