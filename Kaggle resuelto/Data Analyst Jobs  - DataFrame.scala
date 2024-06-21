// Databricks notebook source
val rutaCSV = "/FileStore/tables/DataAnalyst.csv"
//index_col la primera colununa 

val df = spark.read.format("csv")
                            .option("inferSchema", "true") //qure no infiera
                            .option("header", "true")
                            .option("delimiter",",")
                            .option("quote", "\"") //cuando usamos comillas dobles para cadenas
                            .option("escape","\"") //escapamos cdomillas dobles
                            .option("multiline", "true") //para saltos de linea 
                            .load(rutaCSV)
                            .cache()

// COMMAND ----------

df.show(10,50, vertical = true)

// COMMAND ----------

// Ejercicio 1: Funciones de Ventana
// Objetivo: Calcular el salario promedio estimado para cada "Company Name" y asignar
// un ranking basado en este promedio dentro de cada "Industry".
// 1. Limpia la columna Salary Estimate para extraer los valores numéricos.
// 2. Calcula el salario promedio estimado para cada empresa.
// 3. Asigna un ranking a cada empresa dentro de su industria basado en el salario
// promedio

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val windowPaisSector = Window.partitionBy("Company Name", "Industry").orderBy("Salary Estimate Media") //se ordena por salario


val dfSalary = df.withColumn("Salary Estimate",  regexp_replace(
                                                                col("Salary Estimate"), 
                                                                "[^0-9K]", "" //sustituimos todo por un espacio que no sean numeros
                                                                )
                            )
                  .withColumn("arraySalary", split(col("Salary Estimate"), "K" //convertimos en array y sacamos pos 0 y 1 y calculamos la media
                                                        )
                          )
                  .withColumn("Salary Estimate Media",  (col("arraySalary")(0) * 1000 +
                                                  col("arraySalary")(1) * 1000 ) / 2  )
                  .drop("arraySalary") //borramos columnas
            
val dfEmpresa = dfSalary.withColumn("Ranking", 
                                              row_number() //funcion row para establecer un ranking
                                              .over(windowPaisSector)
                                              )



// dfSalary.show(10,50, vertical = true)

dfEmpresa.select("Job Title", "Industry" ,"Company Name", "Salary Estimate", "Ranking").show()


// COMMAND ----------

dfSalary.printSchema

// COMMAND ----------

df.filter(col("Industry") === "Wholesale").filter(  col("Location") === "Centennial, CO").show(500,500, vertical = true)

// COMMAND ----------

// Ejercicio 2: Pivot
// Objetivo: Crear una tabla pivot que muestre el número de empleos disponibles en
// cada "Location" por "Industry".
// 4. Crear una tabla pivot que muestre el conteo de empleos disponibles en cada
// ubicación por industria.
import org.apache.spark.sql.functions._


val dfPivot = df.groupBy("Industry")
                .pivot("Location")
                .count()



dfPivot.show(500,500, vertical = true)

// COMMAND ----------

// Ejercicio 3: Agregaciones Complejas
// Objetivo: Calcular el ingreso total estimado por sector y contar el número de
// compañías en cada sector con un ingreso desconocido.
// 5. Calcular el ingreso total estimado por sector.
// 6. Contar el número de compañías en cada sector con un ingreso desconocido



val dfSalary = df.withColumn("Revenue",  regexp_replace(
                                                                col("Revenue"), 
                                                                "[^0-9$]", "" //sustituimos todo por un espacio que no sean numeros
                                                                )
                            )
                  .withColumn("arraySalary", split(col("Revenue"), "\\$" //convertimos en array y sacamos pos 0 y 1 y calculamos la media
                                                        )
                          )
                  .withColumn("Revenue Media",  (col("arraySalary")(1) * 1000 +
                                                  col("arraySalary")(2) * 1000 ) / 2  )
                  .withColumn("RevenueConocido",  when(col("Revenue").isNotNull , lit( 1)) //ponemos 1 si no hay ingresos
                                                        .otherwise(lit(0))
                                                          )

                  .drop("arraySalary") //borramos columnas



val dfIngresoSector = dfSalary.groupBy("Sector")
                              .agg(  round(avg("Revenue Media"), 2).as("Ingreso estimado"),
                               sum("RevenueConocido").as("Total empresas ingreso conocido") )
                               .orderBy(col("Total empresas ingreso conocido"))



dfIngresoSector
                // .filter(col("Total empresas ingreso conocido") > 0)
                .show(false)


// COMMAND ----------

df.select("Founded").distinct.orderBy("Founded").show()

// COMMAND ----------

// Ejercicio 4: Análisis Temporal
// Objetivo: Analizar la tendencia de la fundación de compañías por década.
// 7. Extraer el año de fundación de la columna Founded.
// 8. Agrupar las compañías por décadas y contar cuántas fueron fundadas en cada
// década.
import org.apache.spark.ml.feature.Bucketizer
import scala.math.BigDecimal
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val minYear = df.filter(col("Founded") >0).select("Founded").distinct.orderBy(col("Founded").asc).distinct.limit(1).head().getInt(0)
val maxYear = df.filter(col("Founded") >0).select("Founded").distinct.orderBy(col("Founded").desc).distinct.limit(1).head().getInt(0)

val decadaMin = ((minYear / 10) * 10).toDouble
val decadaMax = (((maxYear / 10) + 1) * 10).toDouble
val decadas = (BigDecimal(decadaMin) to BigDecimal(decadaMax) by BigDecimal(10)).map(_.toDouble).toArray



val miBucket = new Bucketizer ()
    .setInputCol("Founded")
    .setOutputCol("Decadas")
    .setSplits(decadas)



//quitamos valores -1 
val dfFixed = df.withColumn("Founded", when (col("Founded") === -1, lit(2000))
.otherwise(col("Founded"))

)


val dataDiscretizada = miBucket.transform(dfFixed)
                                .withColumn("Decadas", col("Decadas").cast("int") )

//Creo dataframe con los datos del array de dacadas pos->dacada
// Definir el esquema para el DataFrame
val schema = StructType(Seq(
  StructField("Decadas", IntegerType, nullable = false),
  StructField("Decada", DoubleType, nullable = false)
))

// Crear un DataFrame a partir del array de décadas
val rows = decadas.zipWithIndex.map { case (decada, indice) => Row(indice, decada)}
val decadasDF = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

val rawRespuesta = dataDiscretizada.join(decadasDF,Seq("Decadas"), "left")
                                .withColumn("Decada", col("Decada").cast("int"))



val respuesta  = rawRespuesta.groupBy(col("Decada"))
                              .count()
                              .withColumnRenamed("count", "numEmpresas")
                              .orderBy("Decada")

respuesta.
show(false)


// COMMAND ----------

// Ejercicio 5: Filtrado Avanzado y Agrupación
// Objetivo: Filtrar las empresas con una calificación (Rating) mayor a 4 y agruparlas por
// sector para encontrar el promedio del salario estimado.
// 9. Filtra las empresas con una calificación mayor a 4.
// 10.Agrupa estas empresas por sector.
// 11.Calcula el salario promedio estimado para cada sector.


//Transformamos salario
val dfSalaryTransform = df.withColumn("Salary Estimate",  regexp_replace(
                                                                col("Salary Estimate"), 
                                                                "[^0-9K]", "" //sustituimos todo por un espacio que no sean numeros
                                                                )
                            )
                  .withColumn("arraySalary", split(col("Salary Estimate"), "K" //convertimos en array y sacamos pos 0 y 1 y calculamos la media
                                                        )
                          )
                  .withColumn("Salary Estimate Media",  (col("arraySalary")(0) * 1000 +
                                                  col("arraySalary")(1) * 1000 ) / 2  )


val dfRanting = dfSalaryTransform.filter(col("Rating") > 4)
                                 .groupBy("Sector")
                                 .agg(
                                      round(
                                            avg(col("Salary Estimate Media"))
                                            ,2
                                            ).alias("Salario medio por sector")
                                      )
                                  .orderBy(col("Salario medio por sector"))



dfRanting.show(false)


// dfSalaryTransform.show(500, 500, vertical = true)

// COMMAND ----------

// Ejercicio 7: Análisis de Competidores
// Objetivo: Analizar las compañías que tienen competidores y calcular el promedio de la
// calificación (Rating) de estas compañías en comparación con las que no tienen
// competidores.
// 15.Filtra las compañías que tienen competidores.
// 16.Calcula el promedio de la calificación para estas compañías.
// 17.Compara este promedio con el de las compañías que no tienen competidores.


val ratingCompetitor = df
                          .filter(col("Competitors")  =!= "-1")
                          .groupBy(col("Competitors"), col("Company Name"))
                          .agg(
                              avg(
                                  col("Rating")
                                ).as("Promedio")
                              )


ratingCompetitor.show(false)

// COMMAND ----------

// Ejercicio 8: Transformaciones de Texto
// Objetivo: Analizar la columna Job Description para encontrar las palabras más
// frecuentes en las descripciones de trabajos.
// 18. Tokeniza la columna Job Description.
// 19.Realiza un conteo de las palabras más frecuentes.
// 20.Muestra las palabras más comunes en las descripciones de trabajos.
import org.apache.spark.ml.feature.Tokenizer


//Instanciamos tokenizer 
val tokenizer = new Tokenizer()
                .setInputCol("Job Description")
                .setOutputCol("descripcionToken")

val tokenizedDF = tokenizer.transform(df)

val palabrasToken = tokenizedDF.withColumn("palabrasDescripcion", explode(
                                                                            col("descripcionToken")
                                                                          )
                                          )

val cuentaPalabra = palabrasToken.groupBy(col("palabrasDescripcion"), col("Job Title"))
                                .count()
                                .withColumnRenamed("count", "numVeces")
                                .orderBy(col("numVeces").desc)
                                .filter(not(col("palabrasDescripcion").contains("and")))
                                .filter(not(col("palabrasDescripcion").contains("to")))
                                .filter(not(col("palabrasDescripcion").contains("data")))
                                .filter(not(col("palabrasDescripcion").contains("the")))
                                .filter(not(col("palabrasDescripcion").contains("of")))
                                .filter(not(col("palabrasDescripcion").contains("with")))
                                .filter(not(col("palabrasDescripcion").contains("for")))
                                .filter(not(col("palabrasDescripcion").contains("a")))
                                .filter(not(col("palabrasDescripcion").contains("to")))
                                .filter(not(col("palabrasDescripcion").contains("or")))
                                .filter(not(col("palabrasDescripcion").contains("is")))
                                .where (  length(col("palabrasDescripcion")) > 3)
                            

cuentaPalabra.show()


// COMMAND ----------

// Ejercicio 9: Funciones de Ventana con Condiciones
// Objetivo: Calcular el salario promedio estimado por industria y asignar una
// clasificación dentro de cada industria, pero solo para compañías fundadas después
// del año 2000.
// 21.Filtra las compañías fundadas después del año 2000.
// 22.Calcula el salario promedio estimado por industria.
// 23.Asigna una clasificación basada en este promedio dentro de cada industria.

val windowIndustria = Window.partitionBy("Industry").orderBy("Salary Estimate Media") //se ordena por salario


val df2000 = df.filter(col("Founded") > 2000) 
                .withColumn("Salary Estimate",  regexp_replace(
                                                                col("Salary Estimate"), 
                                                                "[^0-9K]", "" //sustituimos todo por un espacio que no sean numeros
                                                                )
                            )
                  .withColumn("arraySalary", split(col("Salary Estimate"), "K" //convertimos en array y sacamos pos 0 y 1 y calculamos la media
                                                        )
                          )
                  .withColumn("Salary Estimate Media",  (col("arraySalary")(0) * 1000 +
                                                  col("arraySalary")(1) * 1000 ) / 2  )


val promedioIndustria = df2000.withColumn("Ranking", row_number().over(windowIndustria))
                              .orderBy(col("Founded").asc)
                              .orderBy(col("Ranking").asc)


promedioIndustria.select("Job Title","Salary Estimate","Rating",
                         "Company Name","Location","Industry",
                         "Founded", "Salary Estimate Media", "Ranking")
                 .show()



// COMMAND ----------

 // Ejercicio 10: Transformación y Agregación Compleja
// Objetivo: Convertir la columna Revenue a valores numéricos, calcular el ingreso total
// por sector y el número promedio de empleados.
// 24. Limpia y convierte la columna Revenue a valores numéricos.
// 25.Calcula el ingreso total por sector.
// 26.Calcula el número promedio de empleados por sector.

val dfRawRevenue =  df.withColumn("Revenue Raw", when ( col("Revenue") === "Unknown / Non-Applicable",  0  ) //cambiamos valor desconocido por 0 
                                                .otherwise( col("Revenue") )
                                 )
                        .withColumn("millionOrBillion", when ((col("Revenue Raw")).contains("b"), 1000000000 ) //añadimos columna para saber por cuanto hay que 
                                                        .otherwise(1000000)                                    //multiplicar el precio
                                    )
                        .withColumn("Revenue Raw",  regexp_replace(
                                                                   col("Revenue Raw"), 
                                                                   "[^0-9t]", "" //sustituimos todo por un espacio que no sean numeros ni la t
                                                                   )
                                    )
                        .withColumn("Revenue Raw",  regexp_replace( col("Revenue Raw"), 
                                                                                "[^0-9]", "-" //Nuevamente se sustiuye con - todo lo que no sean numeros
                                                                )
                                    )                        
                        .withColumn("Revenue Raw", split( col("Revenue Raw"), "-"  )) //Hacemos un split a partir de -
                        .withColumn("Revenue Raw", when( col("Revenue Raw")(0) === 0, Array(0,0) ) //Si la primera posición es 0, usamos array (0,0)
                                                        .otherwise(col("Revenue Raw"))
                                    )
                        .withColumn("Media revenue", (
                                                      (col("Revenue Raw")(0) * col("millionOrBillion"))  //Calculamos la media de los salarios
                                                       + (col("Revenue Raw")(1))  * col("millionOrBillion")) / 2  
                                    )
                        .withColumn("Raw size",   regexp_replace( col("Size"), "[^0-9]", " " //transformamos la cadenad e tamaño, quitamos todo lo
                                                                )                            // que no sean numeros
                                    )
                          .withColumn("Raw size",   regexp_replace( col("Raw size"), "\\s+", " " //sustituimos los espacios por un solo espacio
                                                               )
                                     )
                         .withColumn("Raw size",   regexp_replace( col("Raw size"), "\\s+", "-" //sustituimos el espacio que hay por -
                                                               )
                                    )
                          .withColumn("ArraySize",   split( 
                                                            col("Raw size"), "-" //Hacemos explit por -
                                                            )
                                    )
                           .withColumn("mediaSize" , when (
                                                            length(col("ArraySize")(1)) === 0, col("ArraySize")(0) //si solo hay una cifra
                                                            )
                                                      .otherwise ( 
                                                             (col("ArraySize")(0)  +col("ArraySize")(1)) / 2  //si hay dos, calculamos la media
                                                        )
                                                            
                                                            )
                        .groupBy(col("Sector")) //Agrupamos por sector
                        .agg(round(avg(col("Media revenue")), 2) .as("salarioSectorMedia"), //Se calcula la media de salarios
                             round(avg(col("mediaSize")), 2) .as("empleadosSectorMedia") //Se calcula la media de empleados
                           )


dfRawRevenue.show(false)