from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField

if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName('My Schema')\
        .master('local[*]')\
        .getOrCreate()

    mydata = [
        (("Saif", "H", "Shaikh"), "OH", "M"),
        (("Neha", "S", ""), "NY", "F"),
        (("Mitali", "", "Kashiv"), "OH", "F"),
        (("Ram", "S", "Shirali"), "NY", "M"),
        (("Aniket", "M", "Mishra"), "NY", "M"),
        (("Tausif", "M", "Shaikh"), "OH", "M")
    ]

    mySchema = StructType([
        StructField('name',
                    StructType([
                        StructField('fname', StringType(), True),
                        StructField('mname', StringType(), True),
                        StructField('lname', StringType(), True)
                    ])),
        StructField('state', StringType(),True),
        StructField('gender', StringType(), True)
    ])

    df1 = spark.createDataFrame(data=mydata,schema=mySchema)
    df2 = spark.createDataFrame(mydata,mySchema)

    df1.show()

    fnameDf = df1.select('name.fname')
    fnameDf.show()









