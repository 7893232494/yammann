from pyspark import SparkContext, SparkConf, sql
from pyspark.sql import Row
import datetime as d
sc = SparkContext.getOrCreate()
sqlContext = sql.SQLContext(sc)
df = sc.parallelize([
                 Row(nama='Roni', umur=27, tingi=168),
                 Row(nama='Roni', umur=6, tingi=168),
                 Row(nama='Roni', umur=89, tingi=168),])

#df.show()
df = df.toDF()
df.show()