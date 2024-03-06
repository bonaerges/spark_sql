import os

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import upper

spark_local_ip = os.environ["SPARK_LOCAL_IP"] = "127.0.1.1"
spark_hostname = os.environ["SPARK_LOCAL_HOSTNAME"] = "sandraclv"
native_hadoop_lib = os.environ["S"] = "sandraclv"
conf = SparkConf()
conf.setAll(
    [
        ("spark.app.name", "OptimizationSPARK"),
        ("spark.sql.streaming.ui.enabled","true"),
        ("spark.driver.maxResultSize","2g"),
        ("spark.local.ip",spark_local_ip),
        ("spark.local.hostname",spark_hostname),
        ("spark.driver.bindAddress",spark_hostname) ,
        ("spark.ui.port","4041")
    ]
)

sessionSpark=SparkSession.builder.config(conf=conf).getOrCreate()
sessionSpark.sparkContext.setLogLevel("ERROR")


# Read a csv with delimiter and a header
dataframe_csv = sessionSpark.read.option("delimiter", ",").option("header", True).csv("./organizations-2000000.csv")
dataframe_csv.createOrReplaceTempView("organizations")


# Reducir el número de particiones a 5
df_coalesced = dataframe_csv.coalesce(5)
df_coalesced.head()

#Uso de repartition para redistribuir los datos:
# Aumentar el número de particiones a 10
df_repartitioned = dataframe_csv.repartition(10)
df_repartitioned.head()
#Persistencia o caché de los DataFrames:


#Uso de funciones SQL incorporadas en lugar de UDFs:
# Usar una función incorporada (upper) en lugar de una UDF para convertir texto a mayúsculas
df_with_upper = dataframe_csv.withColumn('COUNTRY', upper(dataframe_csv['Country']))

# Realizar un broadcast join con una tabla más pequeña
#df_joined = dataframe_csv.join(broadcast(small_df), dataframe_csv['key'] == small_df['key'])

# Detener la sesión de Spark
sessionSpark.stop()