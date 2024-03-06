# Iniciar una sesión de Spark
import os

from pyspark import SparkConf
from pyspark.sql import SparkSession

spark_local_ip = os.environ["SPARK_LOCAL_IP"] = "127.0.1.1"
spark_hostname = os.environ["SPARK_LOCAL_HOSTNAME"] = "sandraclv"
native_hadoop_lib = os.environ["S"] = "sandraclv"
conf = SparkConf()
conf.setAll(
    [
        ("spark.app.name", "DynamicFilteringExample"),
        ("spark.sql.streaming.ui.enabled","true"),
        ("spark.driver.maxResultSize","1g"),
        ("spark.local.ip",spark_local_ip),
        ("spark.local.hostname",spark_hostname),
        ("spark.driver.bindAddress",spark_hostname) ,
        ("spark.ui.port","4041"),
        ("spark.sql.inMemoryColumnarStorage.compressed","true"),
        ("spark.sql.inMemoryColumnarStorage.batchSize","1000")
    ]
)

sessionSpark=SparkSession.builder.config(conf=conf).getOrCreate()
sessionSpark.sparkContext.setLogLevel("ERROR")

# Leer datos desde un archivo JSON en HDFS
df = sessionSpark.read.json("hdfs://path/to/your/jsonfile.json")

# Mostrar el esquema y las primeras filas de los datos
df.printSchema()
df.show()

# Suponiendo que df es tu DataFrame modificado que quieres escribir

# Configura tus credenciales de AWS (esto se debe hacer de manera segura)
sessionSpark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "tu-access-key")
sessionSpark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "tu-secret-key")

# Escribe el DataFrame en formato Parquet en S3
df.write.parquet("s3a://tu-bucket-s3/path/to/output/dir")


# Detener SparkSession
sessionSpark.stop()