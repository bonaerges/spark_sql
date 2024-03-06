import os

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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
        ("spark.ui.port","4041"),
        ("spark.driver.bindAddress",spark_hostname),
        ("spark.sql.inMemoryColumnarStorage.compressed","true"),
        ("spark.sql.inMemoryColumnarStorage.batchSize","1000")
    ]
)

sessionSpark=SparkSession.builder.config(conf=conf).getOrCreate()
sessionSpark.sparkContext.setLogLevel("ERROR")

# Cargar o crear un DataFrame, por ejemplo, 'df'
df = sessionSpark.read.csv("../organizations-2000000.csv", header=True, inferSchema=True)

# Supongamos que tienes una lista de condiciones de filtrado que se determinan en tiempo de ejecución
dynamic_filters = [("Number of employees", ">=", 500), ("Number of employees", "<=", 1000),("Country", "==", "Chile")]

# Definir y Aplicar los filtros dinámicamente
for column, operator, value in dynamic_filters:
    if operator == ">=":
        df = df.filter(col(column) >= value)
    elif operator == "<=":
        df = df.filter(col(column) <= value)
    elif operator == "==":
        df = df.filter(col(column) == value)
    # Añadir más operadores según sea necesario

# Mostrar los resultados después de aplicar los filtros
df.show()

# Detener la sesión de Spark
sessionSpark.stop()