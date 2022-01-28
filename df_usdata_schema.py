from pyspark.sql import SparkSession
from urllib.request import urlopen
from pyspark import SparkConf,SparkContext
from pyspark.sql.functions import explode,col,concat,lit
from pyspark.sql.types import StringType,StructField, StructType,IntegerType,ArrayType,LongType

if __name__ == '__main__':
    sparkconf = SparkConf()
    sc = SparkContext(conf=sparkconf)
    spark = SparkSession.builder\
                .appName("usdata schema")\
                .master('local[*]')\
                .getOrCreate()
    # first_name, last_name, company_name, address, city, county, state, zip, age, phone1, phone2, email, web
    mySchema = StructType([
        StructField('First_Name',StringType(),True),
        StructField('Last_Name',StringType(),True),
        StructField('company_name', StringType(), True),
        StructField('address', StringType(), True),
        StructField('city', StringType(), True),
        StructField('county', StringType(), True),
        StructField('state', StringType(), True),
        StructField('zip',StringType(),True),
        StructField('age',StringType(),True),
        StructField('phone1',StringType(),True),
        StructField('phone2', StringType(), True),
        StructField('email', StringType(), True),
        StructField('web', StringType(), True)
    ])
    df = spark.read.schema(mySchema).option('header',True).csv("file:///home/saif/LFS/datasets/usdata.csv")
    # df.printSchema()
    # df.show()

    df1= df.withColumn("co_address",concat("company_name",lit("|"),"address"))\
                .withColumn("geo_loc",concat("city",lit("-"),"county",lit("-"),"state"))\
                .drop("company_name","address","city","county","state")

    # df1.show(truncate=False)


