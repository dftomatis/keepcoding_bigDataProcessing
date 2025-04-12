import org.apache.spark.sql.SparkSession
import practica.spUtils.SparkUtils.runSparkSession

object Main {
  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = runSparkSession("Keepkoding")

    val df = spark.read
      .option("header", "true")
      .csv("G:\\Mi unidad\\KeepCoding\\Spark_Scala\\trabajo_practico_scala_spark\\examen\\ventas.csv")

    df.show(false)


  }
}