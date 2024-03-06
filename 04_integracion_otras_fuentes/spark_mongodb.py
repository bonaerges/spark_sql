from pyspark.sql import SparkSession

# Crear una SparkSession con el conector MongoDB
spark = SparkSession.builder \
    .appName("MongoDB Example") \
    .config("spark.mongodb.input.uri", "mongodb://username:password@host:port/database.collection") \
    .config("spark.mongodb.output.uri", "mongodb://username:password@host:port/database.collection") \
    .getOrCreate()

# Leer datos de MongoDB
df = spark.read.format("mongo").load()

# Realizar operaciones de consulta y filtro
df_filtered = df.filter(df["tu_columna"] > 10)
df_filtered.show()

# Detener SparkSession
spark.stop()