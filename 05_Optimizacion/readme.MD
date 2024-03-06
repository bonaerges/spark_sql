### 5. **Optimización y Tunning de Consultas**


*   **Técnicas de Optimización**: Cubrir estrategias para optimizar consultas, como el uso de vistas materializadas, particionado, y cacheo.

# 1. Parámetros relacionados con el Manejo de Memoria y Ejecución

# 2. Parámetros para el Manejo de Shuffles

* ### spark.sql.shuffle.partitions
  * Establece el número de particiones que se deben usar para los shuffles en Spark SQL.
  * El valor por defecto es 200, pero puedes ajustarlo según el tamaño de tu cluster y tus datos.
  
* ### spark.executor.memory
  * Configura la cantidad de memoria a usar por cada ejecutor. En este caso, se asignan 2 GB por ejecutor.

* ### spark.executor.cores: Define el número de núcleos a usar en cada ejecutor.
  * #1. Parámetros relacionados con el Manejo de Memoria y Ejecución
  
* ### spark.executor.memory
  * Define la cantidad de memoria a ser utilizada por cada ejecutor de Spark. 
  * Ajustar este valor correctamente puede ayudar a mejorar el rendimiento, especialmente en operaciones 
     que requieren mucha memoria. 
  
* ###  spark.driver.memory
  * Especifica la cantidad de memoria a usar por el driver de Spark. 
  * Es importante para las operaciones que se ejecutan en el nodo del driver, como la recopilación de 
  resultados de una acción.
  
* ###  spark.memory.fraction
  * Este parámetro define la proporción de la memoria del executor que se dedicará a almacenamiento y ejecución 
   (por defecto es 0.6).
  * . Parámetros para el Manejo de Shuffles
  
* ###s park.sql.shuffle.partitions: Define el número de particiones que se deben usar para operaciones de shuffle.
* Por defecto, Spark SQL utiliza 200 particiones, lo cual puede ser ineficiente para grandes o pequeños conjuntos de datos

* ###  spark.default.parallelism
* Controla el número predeterminado de particiones en RDDs retornados por operaciones de#  shuffle como reduceByKey y join.
* 
* # 3. Optimizaciones de Spark SQL

* ### spark.sql.autoBroadcastJoinThreshold
  * Configura el límite máximo (en bytes) para que una tabla se transmita al realizar un join. 
  * El valor por defecto es 10MB. Si se establece en -1, el broadcast join no será utilizado.
 
* ### spark.sql.inMemoryColumnarStorage.batchSize
  * Define el número de filas que se procesarán a la vez en operaciones de almacenamiento en memoria. 
  * Aumentar este valor puede mejorar la memoria y la eficiencia de la CPU a costa de utilizar más memoria.

* # 4. Configuraciones de SerializacióN

* # 5. Parámetros de Red y Comunicación
* ### spark.network.timeout: Configura el tiempo de espera de la red, útil para evitar desconexiones en ambientes 
* on operaciones de larga duración.spark.executor.heartbeatInterval: Configura el intervalo de tiempo entre los latidos del corazón enviados por los ejecutores al driver para informar sobre su estado.   `
