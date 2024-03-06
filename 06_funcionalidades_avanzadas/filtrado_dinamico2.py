import os

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Iniciar una sesión de Spark
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

# Cargar o crear un DataFrame, por ejemplo, 'df'
df = sessionSpark.read.csv("../organizations-2000000.csv", header=True, inferSchema=True)

# Función para aplicar filtro dinámico generalizado
def dynamic_filter(df, conditions):
    """
    Aplica un filtro dinámico a un DataFrame de Spark.

    :param df: DataFrame de Spark a filtrar.
    :param conditions: Lista de condiciones en formato de tuplas (columna, operador, valor).
    :return: DataFrame filtrado.
    """
    filter_expression = " and ".join(
        ["`{0}` {1} {2}".format(col, op, value if isinstance(value, int) else f"'{value}'")
         for col, op, value in conditions]
    )
    return df.filter(filter_expression)

# Ejemplo de uso
conditions = [
    ("Number of employees", '>=', 500),  # Nº empleados mayor o iguala 500
    ("Number of employees", '<=', 1000),  # Nº empleados menor o igual a 1000
    ('Country', '=', 'Chile')  # Ciudad igual a Chile
]

filtered_df = dynamic_filter(df, conditions)
# Mostrar los resultados después de aplicar los filtros
filtered_df.show()

# Detener la sesión de Spark
sessionSpark.stop()