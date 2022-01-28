from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StringType,StructField,StructType,ArrayType
from pyspark.sql.functions import col,row_number,rank,dense_rank,ntile,lag,avg,explode


if __name__ == '__main__':
    spark = SparkSession.builder \
            .appName('window functions') \
            .master('local[*]')\
            .getOrCreate()

    explodeData = [('Saif', ['Java', 'Scala'], {'hair': 'black', 'eye': 'brown'}),
                   ('Mitali', ['Spark', 'Java', None], {'hair': 'brown', 'eye': None}),
                   ('Ram', ['CSharp', ''], {'hair': 'red', 'eye': ''}),
                   ('Wilma', None, None),
                   ('Jatin', ['1', '2'], {})]
    array_df = spark.createDataFrame(data=explodeData,
                                     schema=['name', 'knownLanguages', 'properties'])
    array_df.printSchema()
    array_df.show(truncate=False)

    df2 = array_df.select("name",explode("properties"))
    df2.printSchema()
    df2.show()

    df3 = array_df.select("name",explode("knownLanguages"))
    df3.printSchema()
    df3.show()