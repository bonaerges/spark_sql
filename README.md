## Para los ejercicios usamos el fichero organizations-2000000.csv - bajado de [https://www.datablist.com/learn/csv/download-sample-csv-files ](https://drive.google.com/uc?id=18vlOi20KcMR328ewc2NBsoBNPrV3vL9Q&export=download)de unso 258 MBS

## CATALYST SPARK SQL DIAGRAMA ESTADOS

###  Análisis: 
  Durante e sta fase, Catalyst convierte tu consulta SQL en un árbol de análisis abstracto(AST). Esto implica analizar la sintaxis de la consulta y verificar que las tablas y columnas referenciadas existan.
  
     Ejemplo : 
      Si tu consulta es 
      SELECT nombre FROM usuarios WHERE edad > 30, 
      Catalyst revisará si existe la tabla 'usuarios' 
      y las columnas 'nombre' y 'edad'.(explain plan)
   
   Simil:  Es como leer una receta y entender cada paso que debes seguir.

### Optimización Lógica: 
 Luego Catalyst aplica reglas de optimización que transforman el árbol de análisis en una forma más eficiente, pero lógicamente equivalente. Estas reglas incluyen eliminación de subconsultas redundantes, simplificación de expresiones y empuje de predicados.
      
      Ejemplo : 
      Si tienes una subconsulta innecesaria, como en 
      SELECT nombre FROM (SELECT * FROM usuarios) WHERE edad > 30, Catalyst la simplificará 
      a SELECT nombre FROM usuarios WHERE edad > 30.
     
Simil: Es como modificar la receta para hacerla más eficiente, pero sin cambiar el sabor del pastel.

### Optimización Física: 

Catalyst genera planes de ejecución físicos a partir del plan lógico. Aquí decide cómo se ejecutará la consulta en el clúster: qué operaciones se realizarán en paralelo, cómo se distribuirán los datos, etc.

        Ejemplo : Si tu consulta implica unir dos grandes tablas, 
        Catalyst decidirá la mejor estrategia de unión 
        (como unión de transmisión o unión de mezcla) basada en el tamaño de las tablas y la distribución de los datos.

Simil: Esto es como decidir si usar un horno eléctrico o de gas para hacer tu pastel, basado en cuál es más eficiente para la tarea.

### Generación de Código: 
Finalmente, esta fase convierte el plan de ejecución físico en código ejecutable. Spark utiliza un proceso llamado "WholeStage Codegen" para generar código Java eficiente que se ejecuta en el clúster.

      Ejemplo : Para la consulta SELECT nombre FROM usuarios WHERE edad > 30, 
      Catalyst generará un código Java que realiza la operación de filtro (edad > 30) y 
      selecciona la columna 'nombre', todo optimizado para una ejecución rápida.

 Simil: Es como si el chef experto te diera instrucciones detalladas y personalizadas para hacer el pastel en tu cocina específica, con tus herramientas y condiciones particulares.
