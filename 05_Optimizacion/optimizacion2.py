import os
from pyspark.sql import SparkSession
import datetime


spark_local_ip = os.environ["SPARK_LOCAL_IP"] = "127.0.1.1"
spark_hostname = os.environ["SPARK_LOCAL_HOSTNAME"] = "sandraclv"
native_hadoop_lib = os.environ["S"] = "sandraclv"

sessionSpark = SparkSession.builder \
    .appName("Spark SQL Tuning Example") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "100") \
    .config("spark.default.parallelism", "100") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.local.ip",spark_local_ip) \
    .config("spark.local.hostname",spark_hostname) \
    .config("spark.driver.bindAddress",spark_hostname) \
    .getOrCreate()
sessionSpark.sparkContext.setLogLevel("ERROR")


# Cargar o crear un DataFrame, por ejemplo, 'df'
df = sessionSpark.read.csv("../organizations-2000000.csv", header=True, inferSchema=True)


# Registrar el DataFrame como una vista SQL temporal
df.createOrReplaceTempView("organizations")
start= datetime.datetime.now()
# Realizar una consulta SQL
consulta_resultado = sessionSpark.sql("SELECT * FROM organizations ORDER BY 1 DESC")
total=consulta_resultado.count()
consulta_resultado.show()
end = datetime.datetime.now()
timeDif = end - start
print(f"Select un total de {total} filas, Tiempo de ejecución :  {timeDif}\n")

sessionSpark.stop()