import pandas as pd

# Cargar el archivo CSV
df = pd.read_csv('../organizations-2000000.csv')

# Concatenar el DataFrame consigo mismo
df_concatenado = pd.concat([df, df])

for i in range(1, 5):
    df_concatenado = pd.concat([df_concatenado,df])

# Opcionalmente, puedes guardar el resultado en un nuevo archivo CSV
df_concatenado.to_csv('archivo_concatenado.csv', index=False)