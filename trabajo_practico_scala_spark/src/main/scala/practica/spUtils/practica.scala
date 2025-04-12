package practica.spUtils


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._


object practica {
  /**Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
   Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
   estudiantes (nombre, edad, calificación).
   Realiza las siguientes operaciones:

   Muestra el esquema del DataFrame.
   Filtra los estudiantes con una calificación mayor a 8.
   Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
   */
  def ejercicio1(estudiantes: DataFrame)(implicit spark:SparkSession): DataFrame = {
    import spark.implicits._
    estudiantes.printSchema()

    val filtrados = estudiantes.filter($"calificacion" > 8)

    val ordenados = filtrados.select("nombre", "calificacion")
      .orderBy(desc("calificacion"))

    ordenados

  }

  /**Ejercicio 2: UDF (User Defined Function)
   Pregunta: Define una función que determine si un número es par o impar.
   Aplica esta función a una columna de un DataFrame que contenga una lista de números.
   */

  def ejercicio2(numeros: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val esParUDF = udf((num: Int) => if (num % 2 == 0) "Par" else "Impar")

    val resultado = numeros.withColumn("paridad", esParUDF($"valor"))

    resultado
  }


  /**Ejercicio 3: Joins y agregaciones
   Pregunta: Dado dos DataFrames,
   uno con información de estudiantes (id, nombre)
   y otro con calificaciones (id_estudiante, asignatura, calificacion),
   realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
   */

  def ejercicio3(estudiantes: DataFrame, calificaciones: DataFrame): DataFrame = {
    val joined = estudiantes.join(calificaciones, estudiantes("id") === calificaciones("id_estudiante"))

    val resultado = joined
      .groupBy("nombre")
      .agg(avg("calificacion").as("promedio"))

    resultado
  }

  /**Ejercicio 4: Uso de RDDs
   Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.

   */
  def ejercicio4(palabras: List[String])(implicit spark: SparkSession): RDD[(String, Int)] = {
    val rdd = spark.sparkContext.parallelize(palabras)
    val resultado = rdd
      .map(_.toLowerCase.trim)
      .filter(_.nonEmpty)
      .map(palabra => (palabra, 1))
      .reduceByKey(_ + _)
    resultado
  }


  /**
   Ejercicio 5: Procesamiento de archivos
   Pregunta: Carga un archivo CSV que contenga información sobre
   ventas (id_venta, id_producto, cantidad, precio_unitario)
   y calcula el ingreso total (cantidad * precio_unitario) por producto.
   */

  def ejercicio5(ventas: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    ventas
      .withColumn("ingreso", col("cantidad") * col("precio_unitario"))
      .groupBy("id_producto")
      .agg(round(sum("ingreso"), 2).alias("ingreso_total"))
      .orderBy("id_producto")
  }


}
