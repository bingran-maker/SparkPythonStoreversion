
# (1)executor返回结果给driver的api
# collect()、take()、first()
# driver.memory.size = 1g
# driver.memory.resultMaxSize = 1g
# 最好不要使用此种方式

# (2)输出
# foreach、foreachPartition
# 结果不会返回给driver

# (3)保存文件到外部存储系统
# saveAsTextFile

