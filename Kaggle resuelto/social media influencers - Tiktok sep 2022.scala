// Databricks notebook source
// Ejercicio 1: Cargar y Mostrar el Dataset
// Carga el dataset desde el archivo CSV y muestra las primeras 5 filas.


val rutaCSV = "/FileStore/tables/social_media_influencers___Tiktok_sep_2022.csv"


val df = spark.read.format("csv")
              .option("inferSchema", "true") //dejamos que interfiera en los tipos
              .option("header", "true")
              .option("delimiter", ",")
              .load(rutaCSV)
              .cache() //se guarda en cache

df.limit(5).show()

// COMMAND ----------

df.printSchema

// COMMAND ----------

import org.apache.spark.sql.functions._


// Ejercicio 2: Contar el Número Total de Suscriptores
// Usa foldLeft para contar el número total de suscriptores de todos los Tiktokers en el
// dataset.

//primero transformamos fila de suscribers en numero, para ello con expresion 
//regular sacamos el numero 
val dfSuscribers = df.withColumn("suscriptoresNumber", regexp_replace (
                                                          col("Subscribers"), "[^0-9\\.]", "" //sacamos numero
                                                        )
                                )
                      .withColumn("suscriptoresString", regexp_replace (
                                                          col("Subscribers"), "[0-9\\.]", "" //sacamos K o M
                                                        )
                                 )
                      .withColumn ("suscriptoresMoK", when ( col("suscriptoresString") === "K", 1000) //asignamos valor
                                                      .otherwise( 1000000 )
                                  )
                      .withColumn("calculaSubs", col("suscriptoresNumber") * col("suscriptoresMoK")  ) //calculamos


dfSuscribers.select("`S.no`", "`Tiktoker name`", "`Tiktok name`", "`Subscribers`","calculaSubs" ).show()



//Primero lo convertimos en un RDD


val filasListas = dfSuscribers.select("calculaSubs").collect().toList


// COMMAND ----------

val totalSupcriptores = (filasListas.map(row => row.getAs[Double]("calculaSubs")).foldLeft(0.0)(_ + _) / filasListas.size)


// COMMAND ----------

// Ejercicio 3: Mostrar el Nombre de Cada Tiktoker
// Usa foreach para imprimir el nombre de cada Tiktoker en el dataset.

val nombreTiktoker = df.select("`Tiktok name`").collect().toList

nombreTiktoker.foreach( nombre => 

      println("-- " + nombre.getString(0) + " --")
)

// COMMAND ----------

// Ejercicio 4: Calcular el Promedio de Vistas
// Usa foldLeft para calcular el promedio de vistas de todos los Tiktokers en el dataset.



val dfViewsRaw = df.withColumn("viewsNumber",  regexp_replace (
                                                          col("`Views avg.`"), "[^0-9\\.]", "" //sacamos numero
                                                              )
                              )
                    .withColumn("viewsString", regexp_replace (
                                                          col("`Views avg.`"), "[0-9\\.]", "" //sacamos numero
                                                            )
                                )
                    .withColumn ("viewsMoK", when (col("`Views avg.`") === "K", 1000) //asignamos valor
                                                      .otherwise( 1000000 )
                                  )
                    .withColumn("viewsCalculado", col("viewsNumber") * col("viewsMoK"))


// dfViewsRaw.show()

val filasVisitas = dfViewsRaw.select("viewsCalculado").collect().toList



// COMMAND ----------


val totalVisitas = (filasVisitas.map(row => row.getAs[Double]("viewsCalculado")).foldLeft(0.0)(_ + _) / (filasVisitas.size))

// COMMAND ----------

// Ejercicio 5: Encontrar el Tiktoker con Más Suscriptores
// Usa foldLeft para encontrar el Tiktoker con el mayor número de suscriptores.



val mayorSubs = filasListas.map(row => row.getAs[Double]("calculaSubs")).foldLeft(Double.MinValue)(_ max _) //usamos este valor para filtrar

dfSuscribers.select("`Tiktoker name`").filter(col("calculaSubs") === mayorSubs).show()

// COMMAND ----------

// Ejercicio 10: Contar el Número Total de Comentarios
// Usa foldLeft para contar el número total de comentarios de todos los Tiktokers en el
// dataset.


val commentRaw =   df.withColumn("commentsNumber",  regexp_replace (
                                                          col("`Comments avg.`"), "[^0-9\\.]", "" //sacamos numero
                                                                )
                                )
                    .withColumn("commentsString", regexp_replace (
                                                          col("`Comments avg.`"), "[0-9\\.]", "" //sacamos numero
                                                            )
                               )
                    .withColumn ("commentsMoK", when (col("`Comments avg.`") === "K", 1000) //asignamos valor
                                                      .otherwise( 1000000 )
                                )
                    .withColumn("commentsCalculado", col("commentsNumber") * col("commentsMoK"))

val filasComentarios = commentRaw.select("commentsCalculado").collect().toList



// COMMAND ----------

val totalComentarios = filasComentarios.map(row => row.getAs[Double]("commentsCalculado")).foldLeft(0.0)(_ + _)

// COMMAND ----------

// Ejercicio 14: Filtrar Tiktokers con Más de 10M de Suscriptores
// Usa filter para obtener una lista de Tiktokers que tienen más de 10 millones de
// suscriptores.

val tiktok10M = dfSuscribers.select("`Tiktoker name`").filter(col("calculaSubs")  > 10000000)


tiktok10M.show()

// COMMAND ----------

// Ejercicio 15: Calcular el Promedio de Shares
// Usa foldLeft para calcular el promedio de comparticiones de todos los Tiktokers en
// el dataset.
val shareRaw =   df.withColumn("shareNumber",  regexp_replace (
                                                          col("`Shares avg.`"), "[^0-9\\.]", "" //sacamos numero
                                                                )
                                )
                    .withColumn("shareString", regexp_replace (
                                                          col("`Shares avg.`"), "[0-9\\.]", "" //sacamos numero
                                                            )
                               )
                    .withColumn ("shareMoK", when (col("`Shares avg.`") === "K", 1000) //asignamos valor
                                                      .otherwise( 1000000 )
                                )
                    .withColumn("shareCalculado", col("shareNumber") * col("shareMoK"))

val filasShare = shareRaw.select("shareCalculado").collect().toList



// COMMAND ----------


//calculamos el promedio de comparticiones
val mayorSubs = (
                  (
                  filasShare
                            .map(
                                  row => row.getAs[Double]("shareCalculado")
                                )
                            .foldLeft(0.0)(_ + _)) 
                           / filasShare.size) //usamos este valor para filtrar


// COMMAND ----------

// Ejercicio 16: Mostrar Tiktokers con Más de 1M de Likes
// Usa filter para obtener una lista de Tiktokers que tienen más de 1 millón de likes
// promedio.

val likesRaw =   df.withColumn("likesNumber",  regexp_replace (
                                                          col("`Likes avg.`"), "[^0-9\\.]", "" //sacamos numero
                                                                )
                                )
                    .withColumn("likeString", regexp_replace (
                                                          col("`Likes avg.`"), "[0-9\\.]", "" //sacamos numero
                                                            )
                               )
                    .withColumn ("likeMoK", when (col("`Likes avg.`") === "K", 1000) //asignamos valor
                                                      .otherwise( 1000000 )
                                )
                    .withColumn("likeCalculado", col("likesNumber") * col("likeMoK"))

val tiktoke1MLikes = likesRaw.select("`Tiktoker name`").filter( col("likeCalculado") > 1000000)

tiktoke1MLikes.show()

// COMMAND ----------

// Ejercicio 17: Encontrar el Tiktoker con Más Comparticiones
// Usa foldLeft para encontrar el Tiktoker con el mayor número de comparticiones
// promedio.


val mayorParticiones = filasShare.map(row => row.getAs[Double]("shareCalculado")).foldLeft(Double.MinValue)(_ max _)

val userParticionesMax = shareRaw.select("`Tiktoker name`").filter( col("shareCalculado") === mayorParticiones)

userParticionesMax.show()

// COMMAND ----------

// Ejercicio 18: Calcular el Promedio de Comentarios
// Usa foldLeft para calcular el promedio de comentarios de todos los Tiktokers en el
// dataset.

val promedioComentarios = (totalComentarios / filasComentarios.size)

// COMMAND ----------

// Ejercicio 19: Contar Tiktokers con Más de 5M de Suscriptores
// Usa filter y size para contar cuántos Tiktokers tienen más de 5 millones de
// suscriptores

val tiktokCount5M = dfSuscribers.filter(col("calculaSubs") > 5000000).count()

// COMMAND ----------

// Ejercicio 20: Ordenar Tiktokers por Número de Suscriptores
// Usa sortBy para ordenar los Tiktokers por número de suscriptores en orden
// descendente.


val subSorted = dfSuscribers.orderBy( col("calculaSubs").desc)
                            .select("`Tiktoker name`",
                                    "`Tiktok name`",
                                    "Subscribers",
                                    "`Views avg.`",
                                    "`Comments avg.`",
                                    "`Shares avg.`")
subSorted.show()

// COMMAND ----------

// Ejercicio 21: Mostrar los 10 Tiktokers con Más Vistas
// Usa sortBy y take para mostrar los 10 Tiktokers con más vistas promedio.

val tiktokNombresVistas = dfViewsRaw.orderBy(col("viewsCalculado")).select("`Tiktoker name`").take(10)


tiktokNombresVistas.foreach( nombre => 

      println("  " + nombre.getString(0) + " ")
)

// COMMAND ----------

// Ejercicio 22: Calcular el Total de Comparticiones
// Usa foldLeft para calcular el total de comparticiones de todos los Tiktokers en el
// dataset.


val mayorSubs = filasShare
                            .map(
                                  row => row.getAs[Double]("shareCalculado")
                                )
                            .foldLeft(0.0)(_ + _)

// COMMAND ----------

// Ejercicio 23: Encontrar el Tiktoker con Más Comentarios
// Usa foldLeft para encontrar el Tiktoker con el mayor número de comentarios
// promedio.

val maxComentarios = filasComentarios.map(row => row.getAs[Double]("commentsCalculado")).foldLeft(0.0)(_ max _)

val tiktokerMaxComments = commentRaw.filter(col("commentsCalculado") === maxComentarios).select("`Tiktoker name`")


tiktokerMaxComments.show()