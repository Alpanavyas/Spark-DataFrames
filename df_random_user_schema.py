from pyspark.sql import SparkSession
from urllib.request import urlopen
from pyspark import SparkConf,SparkContext
from pyspark.sql.functions import explode,col
from pyspark.sql.types import StringType,StructField, StructType,IntegerType,ArrayType,LongType

if __name__ == '__main__':
    sparkconf = SparkConf()
    sc = SparkContext(conf=sparkconf)
    spark = SparkSession.builder\
                .appName("random user schema")\
                .master('local[*]')\
                .getOrCreate()

    mySchema = StructType([
        StructField('results',
                    ArrayType(StructType([
                        StructField('user',
                                    StructType([
                                        StructField('gender', StringType(), True),
                                        StructField('name',
                                                    StructType([
                                                        StructField('title', StringType(), True),
                                                        StructField('first', StringType(), True),
                                                        StructField('last', StringType(), True)
                                                    ])),
                                        StructField('location',
                                                    StructType([
                                                        StructField('street', StringType(), True),
                                                        StructField('city', StringType(), True),
                                                        StructField('state', StringType(), True),
                                                        StructField('zip', StringType(), True)
                                                    ])),
                                        StructField('email', StringType(), True),
                                        StructField('username', StringType(), True),
                                        StructField('password', StringType(), True),
                                        StructField('salt', StringType(), True),
                                        StructField('md5', StringType(), True),
                                        StructField('sha1', StringType(), True),
                                        StructField('sha256', StringType(), True),
                                        StructField('registered', StringType(), True),
                                        StructField('dob', StringType(), True),
                                        StructField('phone', StringType(), True),
                                        StructField('cell', StringType(), True),
                                        StructField('TFN', StringType(), True),
                                        StructField('picture',
                                                    StructType([
                                                        StructField('large', StringType(), True),
                                                        StructField('medium', StringType(), True),
                                                        StructField('thumbnail', StringType(), True)
                                                    ]))
                                    ]))
                    ]))
                    ),
        StructField('nationality',StringType(),True),
        StructField('seed',StringType(),True),
        StructField('version',StringType(),True)
    ])


    df1 = spark.read.schema(mySchema).json("file:///home/saif/LFS/datasets/random_users.json")
    df1.show()

    myDF_1 = df1.withColumn("results", explode("results"))
    myDF_1.printSchema()
    myDF_1.show()



# ============================================


# %%
random_userDF = spark.read.format('json').load('file:///home/saif/LFS/datasets/random_users.json',schema= mySchema)
explodedDF = random_userDF.withColumn('results', explode(col('results')))\
    .select('results.user.gender',
            'results.user.name.title',
            'results.user.name.first',
            'results.user.name.last',
            'results.user.location.street',
            'results.user.location.city',
            'results.user.location.state',
            'results.user.location.zip',
            'results.user.email',
            'results.user.username',
            'results.user.password',
            'results.user.salt',
            'results.user.md5',
            'results.user.sha1',
            'results.user.sha256',
            'results.user.registered',
            'results.user.dob',
            'results.user.phone',
            'results.user.cell',
            'results.user.TFN',
            'results.user.picture.large',
            'results.user.picture.medium',
            'results.user.picture.thumbnail',
            'nationality',
            'seed',
            'version')
explodedDF.show()

# %%
readFile = urlopen('https://randomuser.me/api/0.8/?results=10').read().decode('utf-8')
readFile
# %%
myDF = spark.read.json(sc.parallelize([readFile]), multiLine=True, schema=mySchema)
myDF1 = myDF.withColumn('results', explode(col('results'))).select('results.user.gender',
            'results.user.name.title',
            'results.user.name.first',
            'results.user.name.last',
            'results.user.location.street',
            'results.user.location.city',
            'results.user.location.state',
            'results.user.location.zip',
            'results.user.email',
            'results.user.username',
            'results.user.password',
            'results.user.salt',
            'results.user.md5',
            'results.user.sha1',
            'results.user.sha256',
            'results.user.registered',
            'results.user.dob',
            'results.user.phone',
            'results.user.cell',
            'results.user.TFN',
            'results.user.picture.large',
            'results.user.picture.medium',
            'results.user.picture.thumbnail',
            'nationality',
            'seed',
            'version')
myDF1.show()

# %%




































