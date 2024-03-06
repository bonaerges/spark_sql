from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import datetime
import pandas as pd

from pyspark.sql.functions import col

spark_local_ip = os.environ["SPARK_LOCAL_IP"] = "127.0.1.1"
spark_hostname = os.environ["SPARK_LOCAL_HOSTNAME"] = "sandraclv"

conf = SparkConf()
conf.setAll(
    [

        ("spark.app.name", "CSV2Parquet"),
        ("spark.sql.streaming.ui.enabled","true"),
        ("spark.driver.maxResultSize","2g"),
        ("spark.scheduler.mode", "FAIR"),
        ("spark.local.ip",spark_local_ip),
        ("spark.local.hostname",spark_hostname),
    ]
)

sessionSpark=SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
sessionSpark.sparkContext.setLogLevel("ERROR")
dataframe_csv = sessionSpark.read.option("delimiter", ",").option("header", True).csv("archivo_concatenado.csv")
def write_parquet_file():
    start= datetime.datetime.now()
    dataframe_csv.repartition(1).write.mode('overwrite').parquet('tmp/organizations.parquet')
    end = datetime.datetime.now()
    timeDif = end - start
    print(f"Conversion de CSV  Parquet Tiempo de ejecución :  {timeDif}\n")

write_parquet_file()

start= datetime.datetime.now()
dataframe_parquet= sessionSpark.read.parquet('tmp/organizations.parquet')
dataframe_parquet.show()
end = datetime.datetime.now()
timeDif = end - start
print(f"Read Parquet Tiempo de ejecución :  {timeDif}\n")

#Run SQL on files directly

# OBTENER TOTAL FILAS
dataframe_parquet.createOrReplaceTempView("ParquetTable")
start= datetime.datetime.now()
parkSQL = sessionSpark.sql("select count(1) from  ParquetTable").show()
end = datetime.datetime.now()
timeDif = end - start
print(f"Tiempo de ejecución : Total rows {timeDif}\n")

# OBTENER LAS 5 PRIMERAS FILAS
start= datetime.datetime.now()
sessionSpark.sql("select * from ParquetTable limit 5").show()
end = datetime.datetime.now()
timeDif = end - start
print(f"Tiempo de ejecución : First 5 rows {timeDif}\n")

# OBTENER LAS 5 ULTIMAS FILAS
start= datetime.datetime.now()
sessionSpark.sql("SELECT * FROM ParquetTable ORDER BY 1 DESC  LIMIT 5").show()
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
dataframe_parquet.filter(dataframe_parquet.Country=="Chile").show()
end = datetime.datetime.now()
timeDif = end - start
print(f"Tiempo de ejecución : Rows with country Chile {timeDif}")

# Filtros where el dataframe de spark
start= datetime.datetime.now()
dataframe_parquet.select("Country").distinct().show()
end = datetime.datetime.now()
timeDif = end - start
print(f"Tiempo de ejecución :  Rows with diferent country {timeDif}")

#RDD Represents an immutable, partitioned collection of elements that can be operated on in parallel.
# Stop the SparkSession
sessionSpark.stop()