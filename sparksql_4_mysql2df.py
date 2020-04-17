# 以管理员身份打开windows中的mysql服务
# CMD>net start mysql
import os
from pyspark.sql import SparkSession
# os.environ是当前操作系统的所有环境变量，字典。
os.environ["SPARK_HOME"]="D:\\spark-2.4.5-bin-hadoop2.7"
os.environ["JAVA_HOME"]="D:\\Java\\jdk1.8.0_201"

# (1)构建上下文
spark =SparkSession \
    .builder \
    .master("local[*]") \
    .appName("pyspark_sparksql") \
    .getOrCreate()

# (2)读取mysql数据形成DataFrame
url = "jdbc:mysql://127.0.0.1:3306/bearingdb?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai"
table = "bearing"
properties = { 'user' : 'root', 'password' : 'bigdata' }

df = spark.read.jdbc(url=url,table=table,properties=properties)
df.show()
print(df.rdd.getNumPartitions())
df.createTempView("people")
df1 = spark.sql("select count(*) from people")
df1.show()
