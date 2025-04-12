package practica.spUtils

import org.apache.spark.sql.{Row}
import org.apache.spark.sql.types._
import utils.TestInit

class practicaTest extends TestInit {

  "ejercicio1" should "filtrar estudiantes con nota mayor a 8 y ordenarlos por calificación desc" in {

    val schema = StructType(Seq(
      StructField("nombre", StringType, nullable = false),
      StructField("edad", IntegerType, nullable = false),
      StructField("calificacion", DoubleType, nullable = false)
    ))

    val datos = Seq(
      Row("Ana", 20, 9.5),
      Row("Luis", 22, 7.0),
      Row("Sofía", 21, 8.5),
      Row("Tomás", 19, 6.0)
    )

    val dfEntrada = newDf(datos, schema)

    val resultado = practica.ejercicio1(dfEntrada)

    val dfEsperado = newDf(Seq(
      Row("Ana", 9.5),
      Row("Sofía", 8.5)
    ), StructType(Seq(
      StructField("nombre", StringType, nullable = false),
      StructField("calificacion", DoubleType, nullable = false)
    )))

    checkDfIgnoreDefault(resultado, dfEsperado)
  }

  "ejercicio2" should "clasificar los números como Par o Impar" in {

    val schema = StructType(Seq(
      StructField("valor", IntegerType, nullable = false)
    ))

    val datos = Seq(
      Row(1),
      Row(2),
      Row(3),
      Row(4)
    )

    val dfEntrada = newDf(datos, schema)

    val resultado = practica.ejercicio2(dfEntrada)

    val schemaEsperado = StructType(Seq(
      StructField("valor", IntegerType, nullable = false),
      StructField("paridad", StringType, nullable = true)
    ))

    val datosEsperados = Seq(
      Row(1, "Impar"),
      Row(2, "Par"),
      Row(3, "Impar"),
      Row(4, "Par")
    )

    val dfEsperado = newDf(datosEsperados, schemaEsperado)

    checkDfIgnoreDefault(resultado, dfEsperado)
  }

  "ejercicio3" should "hacer join y calcular promedio de calificaciones por estudiante" in {
    val schemaEstudiantes = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("nombre", StringType, nullable = false)
    ))

    val datosEstudiantes = Seq(
      Row(1, "Ana"),
      Row(2, "Luis"),
      Row(3, "Sofía")
    )

    val dfEstudiantes = newDf(datosEstudiantes, schemaEstudiantes)

    val schemaCalificaciones = StructType(Seq(
      StructField("id_estudiante", IntegerType, nullable = false),
      StructField("asignatura", StringType, nullable = false),
      StructField("calificacion", DoubleType, nullable = false)
    ))

    val datosCalificaciones = Seq(
      Row(1, "Matemática", 9.5),
      Row(1, "Historia", 8.5),
      Row(2, "Matemática", 7.0),
      Row(3, "Historia", 9.0),
      Row(3, "Química", 8.0)
    )

    val dfCalificaciones = newDf(datosCalificaciones, schemaCalificaciones)

    val resultado = practica.ejercicio3(dfEstudiantes, dfCalificaciones)

    val dfEsperado = newDf(Seq(
      Row("Ana", 9.0),
      Row("Luis", 7.0),
      Row("Sofía", 8.5)
    ), StructType(Seq(
      StructField("nombre", StringType, nullable = false),
      StructField("promedio", DoubleType, nullable = true)
    )))

    checkDfIgnoreDefault(
      resultado.orderBy("nombre"),
      dfEsperado.orderBy("nombre")
    )

  }

  "ejercicio4" should "contar las ocurrencias de cada palabra" in {
    val palabras = List("hola", "mundo", "hola", "Spark", "spark", "Spark", "Hola")
    val resultado = practica.ejercicio4(palabras)

    val esperado = Map(
      "hola"  -> 3,
      "mundo" -> 1,
      "spark" -> 3
    )

    resultado.collect().toMap shouldBe esperado
  }

  "ejercicio5" should "calcular el ingreso total por producto" in {

    val ruta = getClass.getResource("/ventas.csv").toURI.getPath

    val dfEntrada = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(ruta)

    val resultado = practica.ejercicio5(dfEntrada)

    val dfEsperado = newDf(
      Seq(
        Row(101, 460.0),
        Row(102, 405.0),
        Row(103, 280.0),
        Row(104, 800.0),
        Row(105, 570.0),
        Row(106, 425.0),
        Row(107, 396.0),
        Row(108, 486.0),
        Row(109, 540.0),
        Row(110, 494.0)
      ),
      StructType(Seq(
        StructField("id_producto", IntegerType, nullable = true),
        StructField("ingreso_total", DoubleType, nullable = false)
      ))
    )

    checkDfIgnoreDefault(resultado, dfEsperado)
  }

}

