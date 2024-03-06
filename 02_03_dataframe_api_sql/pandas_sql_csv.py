import pandas as pd
import datetime
import gc


# Lectura del archivo CSV
start = datetime.datetime.now()
dataframe_csv = pd.read_csv("archivo_concatenado.csv", delimiter=",")
end = datetime.datetime.now()
time_diff = end - start
print(f"Lectura CSV Tiempo de ejecución :  {time_diff}\\n")

dataframe_csv.reset_index(drop=True)
dataframe_csv.set_index(keys="Organization Id")
# Obtener total de filas
start = datetime.datetime.now()
total_rows = len(dataframe_csv)
end = datetime.datetime.now()
time_diff = end - start
print(f"Tiempo de ejecución : Total rows {time_diff}\\n")

# Obtener las 5 primeras filas
start = datetime.datetime.now()
first_five_rows = dataframe_csv.head(5)
end = datetime.datetime.now()
time_diff = end - start
print(f"Tiempo de ejecución : First 5 rows {time_diff}\\n")
print(first_five_rows)

# Obtener las 5 últimas filas
start = datetime.datetime.now()
last_five_rows = dataframe_csv.tail(5)
end = datetime.datetime.now()
time_diff = end - start
print(f"Tiempo de ejecución : Last 5 rows {time_diff}\\n")
print(last_five_rows)

# Filtrar por número de empleados y ordenar por fecha de fundación
start = datetime.datetime.now()
filtered_sorted_df = dataframe_csv[dataframe_csv['Number of employees'] > 5000].sort_values('Founded', ascending=False)
end = datetime.datetime.now()
time_diff = end - start
print(f"Tiempo de ejecución : Number of employees > 5000 sort by Found DESC {time_diff}\\n")
print(filtered_sorted_df.head()) # Muestra solo las primeras filas para simplificar

# Filtrar filas con país Chile
start = datetime.datetime.now()
chile_rows = dataframe_csv[dataframe_csv['Country'] == 'Chile']
end = datetime.datetime.now()
time_diff = end - start
print(f"Tiempo de ejecución : Rows with country Chile {time_diff}\\n")
print(chile_rows.head()) # Muestra solo las primeras filas para simplificar

# Obtener países distintos
start = datetime.datetime.now()
distinct_countries = dataframe_csv['Country'].unique()
end = datetime.datetime.now()
time_diff = end - start
print(f"Tiempo de ejecución : Rows with different country {time_diff}\\n")
print(distinct_countries)
