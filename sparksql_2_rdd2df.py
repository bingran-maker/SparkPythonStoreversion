
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

# (2)读取数据形成RDD
sc = spark.sparkContext
rdd = sc.textFile("sparksql_2_rdd2df.txt") \
    .map(lambda line:line.split(",")) \
    .map(lambda arr:(arr[0],int(arr[1])))
print(rdd.collect())
# (3)RDD转换为DataFrame
# (3-1)方式一，基于RDD的反射机制
print("(3-1)RDD转换为DataFrame方式一，基于RDD的反射机制")
print("(3-1-1)不定义列名")
df = rdd.toDF()
df.show()
print("(3-1-2)定义列名")
df1 = rdd.toDF(["name","age"])
df1.show()

# (3-2)方式二，直接给定Schema信息
print("(3-2)RDD转换为DataFrame方式二，直接给定Schema信息")
rdd1 = sc.textFile("sparksql_2_rdd2df.txt") \
    .map(lambda line:line.split(","))

print("(3-2-1)直接给定Schema信息的第一种形式")
from pyspark.sql import Row
people = rdd1.map(lambda arr:Row(name=arr[0],age=arr[1]))
df2 = spark.createDataFrame(people)
df2.show()

print("(3-2-2)直接给定Schema信息的第二种形式")
person = rdd1.map(lambda arr:(arr[0],int(arr[1])))
from pyspark.sql.types import StructType,StructField,StringType,IntegerType

schema = StructType([
    StructField("name",StringType()),
    StructField("age",IntegerType())
])

df3 = spark.createDataFrame(person,schema)
df3.show()






