import os

from pyspark import SparkConf
from pyspark.sql import SparkSession

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
dataframe_csv = sessionSpark.read.option("delimiter", ",").option("header", True).csv("../organizations-2000000.csv")
dataframe_csv.createOrReplaceTempView("organizations")

# Niveles de Almacenamiento
#Storage Level    Space used  CPU time  InMemory  On-disk  Serialized
#--------------------------------------------------------------------
#MEMORY_ONLY          High        Low       Y          N        N
#MEMORY_ONLY_SER      Low         High      Y          N        Y
#MEMORY_AND_DISK      High        Medium    Some       Some     Some
#MEMORY_AND_DISK_SER  Low         High      Some       Some     Y
#DISK_ONLY            Low         High      N          Y        Y

#dfPersist = df.persist(StorageLevel.MEMORY_ONLY)
#dfPersist = df.persist(StorageLevel.MEMORY_ONLY_SER)
#dfPersist = df.persist(StorageLevel.MEMORY_AND_DISK)

# VENTAJAS
# 💰 Rentable: Los cálculos de Spark son muy costosos, por lo que la reutilización de los cálculos se utiliza para
#  ahorrar costes.
# ⏱ Ahorro de tiempo: Reutilizar los cálculos repetidos ahorra mucho tiempo.
# 📈 Tiempo de ejecución: Ahorra tiempo de ejecución del trabajo y podemos realizar más trabajos en el mismo clúster.

#  método cache() lo guarda por defecto en la memoria
#  método persist() se usa para almacenarlo en el nivel de almacenamiento definido por el usuario.

dataframe_csv.unpersist()
dfCache = dataframe_csv.cache()
dfCache.unpersist()

# Persistir un DataFrame en memoria
dataframe_csv.persist()
#dataframe_csv.show()
print(f"Nº elementos en memoria : {dataframe_csv.count()}")
# Realizar operaciones en el DataFrame persistido
# ...
# Despersistir después de su uso
dataframe_csv.unpersist()
print(f"Nº elementos en memoria  tras liberar: {dataframe_csv.count()}")

#Persistirlo en cache
dfCache = dataframe_csv.cache()
#dfCache.show()
print(f"Nº elementos en cache tras insertar: {dfCache.count()}")

# Despersistir después de su uso
dfCache.unpersist()
#dfCache.show()
print(f"Nº elementos en cache tras liberar: {dfCache.count()}")

# Detener la sesión de Spark
sessionSpark.stop()