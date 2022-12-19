from time import time

from pandas import DataFrame
def extacrt_using_pyspark( spark):
    try:
        x=[]
        y=[]
        yy = [100,1000, 10000,10000, 10000, 100000,1000000,10000000]
        tt = 0
        for i in yy:
            t=time()
            df = spark.read \
                .format("jdbc") \
                .option("url", "jdbc:mysql://localhost:3306/analyticdemo") \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .option("dbtable", "video_measure") \
                .option("user", "root") \
                .option("password", "mukul123") \
                .load()
            dt = DataFrame(df.head(i))
            x.append(time()-t)
            y.append(tt)
            tt+=1
            print(df.columns)
        df = DataFrame({
            'seq': y,
            'time': x,
            'rows': yy
        })
        print(df.head(10))
        df.plot.line()

    except Exception as e:
        print(e)
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.jars", "/Users/mukul/Documents/mysql-connector-java-8.0.222/mysql-connector-java-8.0.22.jar") \
    .master("local").appName("PySpark_MySQL_test").getOrCreate()
print(spark)
extacrt_using_pyspark(spark)