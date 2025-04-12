# 🧠 Práctica con Apache Spark + Scala

Este proyecto contiene una serie de ejercicios resueltos con **Apache Spark usando Scala**, enfocados en:

- Operaciones básicas con DataFrames y RDDs
- Funciones definidas por el usuario (UDF)
- Joins y agregaciones
- Lectura de archivos CSV
- Escritura y validación de tests con **ScalaTest**

---

## 🗂 Estructura del proyecto

```
src/
├── main/
│   └── scala/
│       └── practica/
│           └── practica.scala   <-- Implementación de los ejercicios (objeto `practica`)
├── test/
│   ├── resources/
│   │   └── ventas.csv           <-- Archivo CSV usado en el ejercicio 5
│   └── scala/
│       └── practica/
│           └── practicaTest.scala   <-- Tests de todos los ejercicios
│       └── utils/
│           └── TestInit.scala       <-- Inicialización del entorno de test (SparkSession)
```

---

## 🚀 Requisitos

| Componente    | Versión mínima recomendada |
|---------------|----------------------------|
| Scala         | 2.12.x                     |
| Apache Spark  | 3.4.0                      |
| sbt           | 1.5+                       |
| Java JDK      | 11 o 17                    |
| ScalaTest     | 3.2.x                      |

---

## ⚙️ Dependencias sugeridas (build.sbt)

Asegurate de tener algo similar en tu `build.sbt`:

```scala
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.4.0" % "provided",
  "org.scalatest" %% "scalatest" % "3.2.18" % "test"
)
```

> 🔁 Si usás IntelliJ, recordá activar el plugin de **Scala** y sincronizar el proyecto SBT.

---

## 🧪 Ejecutar los tests

Desde la terminal, ubicado en la raíz del proyecto:

```bash
sbt test
```

O, desde IntelliJ:

1. Clic derecho sobre la clase `practicaTest.scala`
2. Seleccionar: `Run 'practicaTest'`

---

## 📝 Detalle de ejercicios

1. **Ejercicio 1**: Operaciones con DataFrames (filtro y ordenamiento por calificación)
2. **Ejercicio 2**: UDF para determinar si un número es par o impar
3. **Ejercicio 3**: Join de estudiantes con calificaciones y cálculo del promedio
4. **Ejercicio 4**: Uso de RDD para contar ocurrencias de palabras
5. **Ejercicio 5**: Lectura de un CSV y cálculo del ingreso total por producto

---

## 📁 Archivo CSV de prueba

El archivo `ventas.csv` se encuentra en:

```
src/test/resources/ventas.csv
```

> ⚠️ Es importante no moverlo ni renombrarlo, ya que los tests acceden a él dinámicamente mediante `getClass.getResource(...)`.

---

## ✅ Autor

Desarrollado por **Ing. Dario Tomatis** como parte de prácticas con Spark y Scala.

---
