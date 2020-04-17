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

# (2)读取数据形成RDD
sc = spark.sparkContext
rdd = sc.textFile("sparksql_3_df2mysql.txt") \
    .map(lambda line:line.split(",")) \
    .map(lambda arr:(arr[0],int(arr[1])))
print(rdd.collect())
# (3)RDD转换为DataFrame,基于RDD的反射机制
print("(3-1)RDD转换为DataFrame方式一，基于RDD的反射机制")
print("(3-1)定义列名")
df1 = rdd.toDF(["name","age"])
df1.show()

url = "jdbc:mysql://127.0.0.1:3306/mycount?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai"
table = "person"
mode = "overwrite"
properties = { 'user' : 'root', 'password' : 'bigdata' }
df1.write.jdbc(url=url,table=table,mode=mode,properties=properties)
# 没有表会自动创建。



# java.sql.SQLException: The server time zone value '�й���׼ʱ��' is unrecognized or represents more than one time zone.
# You must configure either the server or JDBC driver
# (via the 'serverTimezone' configuration property) to use a more specifc time zone value if you want to utilize time zone support.
#报错说是时区不对,因为mysql-connection-java版本导致时区的问题。
# mysql-connector-java-8.0.17.jar
# 解决方式：在连接数据库的配置文件中加上以下，时区亚洲/上海



