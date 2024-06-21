// Databricks notebook source
val rutaCSV = "/FileStore/tables/movies2.csv"

// 1. Carga del Dataset:
// • Carga el archivo movies2.csv en un DataFrame de Spark y muestra los
// primeros 5 registros.

//Cargamos el dataset
val df = spark.read.format("csv")
              .option("inferSchema", "true") //dejamos que interfiera en los tipos
              .option("header", "true")
              .option("delimiter", ",")
              .option("mode", "DROPMALFORMED") //Si no hay filas con formato correcto, se borran
              .load(rutaCSV)
              .cache() //se guarda en cache
  

// COMMAND ----------

df.printSchema

// COMMAND ----------

df.show(100, 100, vertical = true)

// COMMAND ----------

// 2. Conteo de Registros:
// • Calcula y muestra el número total de registros en el DataFrame.


df.count()

// COMMAND ----------

import org.apache.spark.sql.functions._


// 3. Películas por Año:
// • Crea un nuevo DataFrame que contenga el número de películas lanzadas
// por año. Muestra los resultados ordenados por año en orden ascendente.

//Cogemos la columna release_date, la pasamos a tipo date, luego sacamos el año y se guarda en la columna year
//Hacemos un group by a partir de esa columna y agregamos count


val dfPeliculasYear = df.withColumn("release_date", to_timestamp(
                                                            col("release_date"), "yyyy-MM-dd"
                                                            )
                                )
                    .withColumn("Year", year(
                                            col("release_date")
                                            )
                                )
                    .groupBy(col("Year"))
                    .count()
                    .orderBy(col("Year"))

dfPeliculasYear.show()

// COMMAND ----------

// 4. Promedio de Popularidad por Año:
// • Calcula el promedio de popularidad de las películas por año y muestra
// los resultados ordenados por año


val dfPeliculasPopularity = df.withColumn("release_date", to_timestamp(
                                                            col("release_date"), "yyyy-MM-dd"
                                                            )
                                )
                              .withColumn("Year", year(
                                                      col("release_date")
                                                      )
                                          )
                                .groupBy(col("Year"))
                                .agg(
                                      round(
                                            avg(
                                              col("popularity")
                                            ),
                                            2
                                      ).as("Media_popularidad")
                                    )
                                .orderBy(col("Year"))


dfPeliculasPopularity.show()

// COMMAND ----------

// 5. Películas Más Populares:
// • Encuentra y muestra las 10 películas con mayor popularidad.


val dfTopMovies = df
                    .orderBy(
                              col("popularity").desc
                            )
                    .limit(10)


dfTopMovies.show(100, 100, vertical = true)

// COMMAND ----------

// 6. Promedio de Votos por Género:
// • Explota la columna genre_names para que cada género tenga su propio
// registro y luego calcula el promedio de votos (vote_average) para cada
// género


//Primero paso a array la columna


val dfGenerosVotos = df.withColumn( "arrayGeneros",
                                                  split( //la cadena la pasamos a array con , 
                                                        regexp_replace(
                                                                        col("genre_names"),"\\[|\\]", "" //sustituimos llaves por espacios
                                                                      )
                                                  , ", ")
                                
                                )
                        
val dfGenerosVotos02 = dfGenerosVotos.withColumn(
                                            "generosNombres",
                                            explode(col("arrayGeneros"))
                                          )
                                .groupBy(col("generosNombres"))
                                .agg(
                                      round(
                                            avg(col("vote_average"))
                                            ,2
                                            ).as("MediaVotosGenero")
                                    )

dfGenerosVotos02.show()

// COMMAND ----------

dfGenerosVotos.show()

// COMMAND ----------

dfGenerosVotos.show()

// COMMAND ----------

// 7. Número de Películas por Género:
// • Calcula el número de películas en cada género y muestra los resultados
// ordenados de mayor a menor.

val numGeneros = dfGenerosVotos
                              .withColumn(
                                            "generosNombres",
                                            explode(col("arrayGeneros"))
                                          )

                              .groupBy(
                                      col("generosNombres")
                                      )
                              .count()
                              .withColumnRenamed("count", "numPeliculas")
                              .orderBy(col("numPeliculas").desc)


numGeneros.show()


// COMMAND ----------

// 8. Películas con Más de 10000 Votos:
// • Encuentra y muestra todas las películas que tienen más de 10,000 votos.



val peliMilVotos = df.filter(col("vote_count") > 10000)
                      .orderBy(col("vote_count").asc)


peliMilVotos.show()

// COMMAND ----------

// 9. Filtrar Películas por Año:
// • Filtra y muestra todas las películas lanzadas después del año 2000.


val pelis2000 = df.withColumn("release_date", to_timestamp(
                                                            col("release_date"), "yyyy-MM-dd"
                                                            )
                                )
                              .withColumn("Year", year(
                                                      col("release_date")
                                                      )
                                          )
                              .filter(col("year") > 2000)
                              .orderBy(col("year").asc)
                              .drop(col("Year"))

pelis2000.show()

// COMMAND ----------

// 10.Películas con Título Más Largo:
// • Encuentra y muestra las 5 películas con los títulos más largos (en
// términos de número de caracteres).


val dfTitulosLargos = df.orderBy(
                                  length(col("original_title")).desc
                                )
                          .withColumn("longitudTitulo", length(col("original_title")) )
                          .limit(5) //5 películas

dfTitulosLargos.show()