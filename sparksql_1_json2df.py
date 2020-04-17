


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

# (2)读取数据形成DataFrame
path = "data_test_simple.json"
df = spark.read.json(path)
df.show()

# DSL语法的使用,DataFrame是RDD和二维表的联合抽象
df.printSchema()
df.select("SN").show()
df.select(df["SN"].alias("name1"),df["SNInfo"].alias("name2")).show()

# (3)创建一张临时表
# 1.6.X版本
df.registerTempTable("people")
# 2.4.5版本
df.createTempView("person")
df.createOrReplaceTempView("person")

spark.sql("select * from people").show()
spark.sql("select * from person").show()

# (4)全局临时视图
df.createGlobalTempView("person2")

# spark.sql("select * from person2").show()
# Table or view not found: person2全局临时视图没有找到
spark.sql("select * from global_temp.person2").show()
# 注意，全局临时视图默认在global_temp数据库中，所以需要指定数据库名称

print("全局临时视图")
spark1 = spark.newSession()
spark1.sql("select * from global_temp.person2").show()
spark1.sql("select * from person").show()
# Table or view not found: person，因为person临时表只能在spark中读到







