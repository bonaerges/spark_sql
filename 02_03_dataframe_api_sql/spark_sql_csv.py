from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import datetime

from pyspark.sql.functions import col

spark_local_ip = os.environ["SPARK_LOCAL_IP"] = "127.0.1.1"
spark_hostname = os.environ["SPARK_LOCAL_HOSTNAME"] = "sandraclv"
native_hadoop_lib = os.environ["S"] = "sandraclv"
conf = SparkConf()
conf.setAll(
    [
        ("spark.app.name", "Python Spark SQL basic example"),
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

start= datetime.datetime.now()
# Read a csv with delimiter and a header
dataframe_csv = sessionSpark.read.option("delimiter", ",").option("header", True).csv("archivo_concatenado.csv")
dataframe_csv.createOrReplaceTempView("organizations")

end = datetime.datetime.now()
timeDif = end - start
print(f"Lectura CSV Tiempo de ejecución :  {timeDif}\n")


#Run SQL on files directly

# OBTENER TOTAL FILAS
start= datetime.datetime.now()
sessionSpark.sql("select count(1) from organizations").count()
end = datetime.datetime.now()
timeDif = end - start
print(f"Tiempo de ejecución : Total rows {timeDif}\n")

# OBTENER LAS 5 PRIMERAS FILAS
start= datetime.datetime.now()
sessionSpark.sql("select * from organizations limit 5").show()
end = datetime.datetime.now()
timeDif = end - start
print(f"Tiempo de ejecución : First 5 rows {timeDif}\n")

# OBTENER LAS 5 ULTIMAS FILAS
start= datetime.datetime.now()
sessionSpark.sql("SELECT * FROM organizations ORDER BY 1 DESC  LIMIT 5").show()
end = datetime.datetime.now()
timeDif = end - start
print(f"Tiempo de ejecución : Last 5 rows {timeDif}")

# Perform DataFrame API operations
start= datetime.datetime.now()
dataframe_csv.select("*") \
    .filter(col("Number of employees") > 5000) \
    .orderBy(col("Founded").desc()).show()

end = datetime.datetime.now()
# Show the result
timeDif = end - start
print(f"Tiempo de ejecución : Number of employees > 50000 sort by Found DESC {timeDif}")

# Filtros sobre el dataframe de spark
start= datetime.datetime.now()
dataframe_csv.filter(dataframe_csv.Country=="Chile").show()
end = datetime.datetime.now()
timeDif = end - start
print(f"Tiempo de ejecución : Rows with country Chile {timeDif}")

# Filtros where el dataframe de spark
start= datetime.datetime.now()
dataframe_csv.select("Country").distinct().show()
end = datetime.datetime.now()
timeDif = end - start
print(f"Tiempo de ejecución :  Rows with diferent country {timeDif}")

#RDD Represents an immutable, partitioned collection of elements that can be operated on in parallel.
# Stop the SparkSession
sessionSpark.stop()