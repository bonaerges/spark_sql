# UDFs (Funciones Definidas por el Usuario): Mostrar cómo crear y usar UDFs para extender
# la funcionalidad de Spark SQL.
# las UDFs pueden ser menos eficientes que las funciones nativas de Spark, ya que no se optimizan en el mismo grado.
#  Utilízalas cuando no haya una función integrada que realice la tarea que necesitas.
import os
from datetime import datetime

from numpy import average
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, FloatType
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark_local_ip = os.environ["SPARK_LOCAL_IP"] = "127.0.1.1"
spark_hostname = os.environ["SPARK_LOCAL_HOSTNAME"] = "sandraclv"
native_hadoop_lib = os.environ["S"] = "sandraclv"
conf = SparkConf()
conf.setAll(
    [
        ("spark.app.name", "UDFExample"),
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

# Cargar o crear un DataFrame, por ejemplo, 'df'
df = sessionSpark.read.csv("../organizations-2000000.csv", header=True, inferSchema=True)

# UDF 1 genera una descripción corta de cada organización basada en su nombre, industria y número de empleados.
def short_description(name, industry, num_employees):
    return f"{name}, operating in {industry}, has approximately {num_employees} employees."

short_description_udf = udf(short_description, StringType())
sessionSpark.udf.register("shortDescUDF", short_description_udf)

# usar la UDF para añadir una nueva columna con la descripción corta.

df_with_description = df.withColumn("ShortDescription",
                                    short_description_udf(df["Name"], df["Industry"], df["Number of employees"]))
df_with_description.show()

#UDF 2: Antiguedad en años de la empresa
def calculate_age(founded_year):
    current_year = datetime.now().year
    return current_year - founded_year if founded_year else None

# Registrar UDF
age_udf = udf(calculate_age, IntegerType())
sessionSpark.udf.register("ageUDF", age_udf)

# Aplicar UDF y crear nueva columna 'Age'
result_df = df.withColumn("Age", age_udf(df["Founded"]))

# Mostrar los resultados
result_df.show()

sessionSpark.stop()