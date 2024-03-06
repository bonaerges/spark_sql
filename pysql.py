import pandasql as ps
import pandas as pd
from datetime import datetime

start = datetime.now()
df = pd.read_csv(filepath_or_buffer="organizations-2000000.csv")
end = datetime.now()
timeDif = end - start
print(f"Lectura CSV Tiempo de ejecución :  {timeDif}\n")

# OBTENER LAS 5 PRIMERAS FILAS
start = datetime.now()
# Execute the query using pandasql
query = "SELECT * FROM df LIMIT 5"
resultado = ps.sqldf(query, globals())

end = datetime.now()
print(resultado)
time = end - start
print("\n")
print(f"Tiempo de ejecución :  {time}")

# OBTENER LAS 5 ULTIMAS FILAS
start = datetime.now()
# Execute the query using pandasql
query = "SELECT * FROM df ORDER BY 1 DESC  LIMIT 5"
resultado = ps.sqldf(query, globals())

end = datetime.now()
print(resultado)
time = end - start
print("\n")
print(f"Tiempo de ejecución :  {time}")
