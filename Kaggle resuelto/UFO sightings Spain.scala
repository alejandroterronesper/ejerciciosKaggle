// Databricks notebook source
// Ejercicio 1: Cargar y Mostrar el Dataset
// Carga el dataset desde el archivo CSV y muestra las primeras 10 filas.


val rutaCSV = "/FileStore/tables/UFO_sightings_Spain.csv"


val df = spark.read.format("csv")
              .option("inferSchema", "true") //dejamos que interfiera en los tipos
              .option("header", "true")
              .option("delimiter", ",")
              .load(rutaCSV)
              .cache() //se guarda en cache

df.limit(10).show()

// COMMAND ----------

// Ejercicio 2: Contar el Número Total de Incidentes de Vacas
// Usa foldLeft para contar el número total de incidentes de vacas en el dataset.

val indicentesVacas = df.filter( $"`Cow incidents`" === "Yes").collect().toList  //sacamos las filas con incidentes de vacas





// COMMAND ----------

val numVacas = indicentesVacas.foldLeft(0)( (contador, _) => contador + 1 )


// COMMAND ----------

// Ejercicio 3: Mostrar las Coordenadas de los Avistamientos
// Usa foreach para imprimir las coordenadas (longitud y latitud) de cada avistamiento
// en el dataset.

val filasLongitudLatitud = df.select("longitude", "latitude").collect().toList



// COMMAND ----------

val cadenaPosicion = filasLongitudLatitud.foldLeft(()){ (_, fila) => 
        val longitud = fila.getAs[Double]("longitude")
        val latitude = fila.getAs[Double]("latitude")


        print("\nLongitud: " + longitud + " - Latitud: " + latitude + "\n") 
} 

// COMMAND ----------

import org.apache.spark.sql.functions._


// Ejercicio 4: Calcular el Promedio de Incidentes de Vacas por Año
// Usa groupBy y map para calcular el promedio de incidentes de vacas por año.

val incidentesYear = df.filter($"`Cow incidents`" === "Yes").groupBy("year", "Cow incidents").count()


val incidentesYearLista = incidentesYear.collect().toList


val totalIncidentes = incidentesYearLista.map(_.getLong(2)).sum

val promedioVacasYear = totalIncidentes / (incidentesYearLista.size)

// COMMAND ----------

print("Promedio de incidentes: " + promedioVacasYear)

// COMMAND ----------

// Ejercicio 5: Encontrar el Año con Más Círculos de Cultivo
// Usa groupBy y foldLeft para encontrar el año con el mayor número de círculos de
// cultivo encontrados.

val incidentesCultivosYear = df.filter(col("`Crop circle found`") === "Yes").groupBy("Year","Crop circle found").count()
//Ahora creamos lista
val incidentesCultivoYearLista = incidentesCultivosYear.collect().toList 

//con fold sacamos el nº mayor de incidencias 
val mayorIncidenteYear = incidentesCultivoYearLista.map(row => row.getAs[Long]("count")).foldLeft(Double.MinValue)(_ max _) //usamos este valor para filtrar

//casteamos 
val castMayorIncidenteYear = mayorIncidenteYear.toLong

//filtramos para sacar las filas
val aniosMayorCultivo = incidentesCultivosYear
                                              .filter(col("count") === castMayorIncidenteYear)
                                              .select("Year")

//resultado
aniosMayorCultivo.show()

// COMMAND ----------

// Ejercicio 6: Filtrar Avistamientos con Aliens
// Usa filter para obtener una lista de avistamientos en los que se avistaron aliens


val filtroAvistamientoAliens = df.filter(col("Alien sighted") === "Yes")

filtroAvistamientoAliens.show()


// COMMAND ----------

// Ejercicio 7: Calcular el Número Total de Secuestros
// Usa foldLeft para calcular el número total de eventos de secuestro en el dataset.

// val numVacas = indicentesVacas.foldLeft(0)( (contador, _) => contador + 1 )

//primero filtramos los secuestros 
val filterAbduction = df.filter(col("Abduction event") === "Yes").collect().toList


val numAbducciones = filterAbduction.foldLeft(0)( (contador, _) => contador +1  )

print("Número de abducciones totales: "  + numAbducciones + "\n\n")


// COMMAND ----------

// Ejercicio 8: Ordenar Avistamientos por Año
// Usa sortBy para ordenar los avistamientos por año en orden ascendente


val avistamientosYear = df
                          .filter(col("`Alien sighted`") === "Yes")
                          .orderBy(col("Year").asc) 



avistamientosYear.show()

// COMMAND ----------

// Ejercicio 9: Mostrar los 5 Años con Más Avistamientos de Aliens
// Usa groupBy, map y sortBy para mostrar los 5 años con más avistamientos de aliens.


val avistamientosAlinesYear = df.filter(col("`Alien sighted`") === "Yes").groupBy("year","Alien sighted" ).count()
val avistamientosAlinesYearLista = avistamientosAlinesYear.collect().toList
val maxIncidentes = incidentesYearLista.map(_.getLong(2)).max
val incidentes5Year = avistamientosAlinesYear.filter(col("count") === maxIncidentes).limit(5)


incidentes5Year.show()

// COMMAND ----------

// Ejercicio 10: Filtrar Avistamientos en una Longitud Específica
// Usa filter para obtener una lista de avistamientos que ocurrieron en una longitud
// específica.
import org.apache.spark.sql.DataFrame



val longEspecifica : Double = 5.380504002188364
// df.show()


def avistamientosLongEspecifica (longitud: Double): DataFrame = {
    df
  .filter(col("longitude")  < longitud && col("Alien sighted") === "Yes")
  .orderBy(col("longitude").desc)


} 


// COMMAND ----------

val dfLongitud = avistamientosLongEspecifica(longEspecifica)
dfLongitud.show(false)

// COMMAND ----------

// Ejercicio 11: Contar Avistamientos con Círculos de Cultivo y Aliens
// Usa filter para contar el número de avistamientos que reportaron tanto círculos de
// cultivo como avistamientos de aliens.


val numCultivoAliens = df.filter(col("Crop circle found") === "Yes" && col("Alien sighted") === "Yes").count()
print("Número de avistamientos en cultivos y aliens: "  + numCultivoAliens)

// COMMAND ----------

// Ejercicio 12: Calcular el Promedio de Secuestros por Año
// Usa groupBy y foldLeft para calcular el promedio de eventos de secuestro por año.

// df.show()

val secuestroYear = df
                      .filter(col("`Abduction event`") === "Yes")
                      .groupBy("Year", "Abduction event")
                      .count()
                      .collect()
                      .toList


// val totalSecuestros = secuestroYear.



// COMMAND ----------

secuestroYear

// COMMAND ----------

val secuestrosTotales = secuestroYear.map(row => row.getAs[Long]("count")).foldLeft(0.0)(_+_) 
val promedioSecuestros = secuestrosTotales / secuestroYear.size


// COMMAND ----------

// Ejercicio 13: Encontrar la Latitud con Más Avistamientos
// Usa groupBy y foldLeft para encontrar la latitud con el mayor número de
// avistamientos.


val latitudAvistamientos  = df.filter(col("`Alien sighted`") === "Yes").groupBy("latitude").count()
val latitudAvistamientosListas = latitudAvistamientos.collect.toList
val maxAvistamientoLatitud = latitudAvistamientosListas.map(row => row.getAs[Long]("count")).foldLeft(0.0)(_ max _) 
val latitudMaxima = latitudAvistamientos.filter(col("count") === maxAvistamientoLatitud)
latitudMaxima.orderBy(col("latitude").desc).show()

// COMMAND ----------



// COMMAND ----------

// Ejercicio 14: Filtrar Avistamientos en un Rango de Años
// Usa filter para obtener una lista de avistamientos que ocurrieron en un rango
// específico de años.

val anioRango = df.filter( col("`Alien sighted`") === "Yes" && col("Year") > 1500 &&  col("Year")< 1900)
anioRango.show()


val anios = List(1999,1714, 1595)

val anioConjunto= df.filter( col("`Alien sighted`") === "Yes" && col("Year").cast("int").isin(anios: _*))
anioConjunto.show()

// COMMAND ----------

//probando diferencia entre where y filter

val numFilter  = df.filter(col("`Alien sighted`") === "Yes" && col("Year") > 1500 &&  col("Year") < 1900).count()
val numWhere  = df.where(col("`Alien sighted`") === "Yes" && col("Year") > 1500 &&  col("Year") < 1900).count()



// COMMAND ----------

df.printSchema

// COMMAND ----------

df.show()

// COMMAND ----------

// Ejercicio 15: Contar Avistamientos en una Latitud y Longitud Específica
// Usa filter para contar el número de avistamientos que ocurrieron en una latitud y
// longitud específica


def avistamientosLongLatEspecifica (longitud: Double, latitud: Double): DataFrame = {


  val filtratoLatitud = df.filter(col("longitude") === longitud || col("latitude") === latitud)
  filtratoLatitud


}

// COMMAND ----------

val filtrado = avistamientosLongLatEspecifica(-7.5363009278251045, 42.21254655263939)

filtrado.show()

// COMMAND ----------

// Ejercicio 16: Calcular el Promedio de Círculos de Cultivo por Año
// Usa groupBy y map para calcular el promedio de círculos de cultivo encontrados por
// año.

val cultivoYear = df
  .filter(col("`Crop circle found`") === "Yes")
  .groupBy("year", "Crop circle found")
  .count()


val cultivoYearLista = cultivoYear.collect().toList


//Ahora se hace un map
val totalCultivoYear = cultivoYearLista.map(_.getLong(2)).sum

val promedioCultivo  = totalCultivoYear / (cultivoYearLista.length)

print ("\nPromedio de circulos de cultivo por año: " + promedioCultivo + "\n")


// COMMAND ----------

df.select("latitude").distinct.show()

// COMMAND ----------

// Ejercicio 17: Mostrar Avistamientos en una Latitud Específica
// Usa filter para obtener una lista de avistamientos que ocurrieron en una latitud
// específica.

def avistamientosLatitud (latitud: Double): DataFrame = {

    val respuesta = df.filter(col("`Alien sighted`") === "Yes" && col("latitude") === latitud)


     respuesta

}

val valor : Double = 42.45146055717676

val filtradoLatitud = avistamientosLatitud(valor)
filtradoLatitud.show()

// COMMAND ----------

// Ejercicio 18: Contar el Número de Años con Avistamientos de Aliens
// Usa groupBy y filter para contar el número de años en los que se reportaron
// avistamientos de aliens.

val numYearAvistamientos = df.filter(col("Alien sighted") === "Yes").groupBy("Alien sighted", "Year")
                                                                    .count()
                                                                    .count() //contamos el número de años 



// COMMAND ----------

// Ejercicio 19: Encontrar la Longitud con Más Incidentes de Vacas
// Usa groupBy y foldLeft para encontrar la longitud con el mayor número de
// incidentes de vacas.


val vacaIncidente = df.filter(col("`Cow incidents`") === "Yes").groupBy("longitude", "Cow incidents").count()
val vacaIncidenteLista = vacaIncidente.collect().toList


val maxIncidenteVaca = vacaIncidenteLista.map(row => row.getAs[Long]("count")).foldLeft(0.0)(_ max _) 


//Ahora filtramos
val lontitudVaca = vacaIncidente
                                .filter(col("count") === maxIncidenteVaca)
                                .select("longitude")
lontitudVaca.show()

// COMMAND ----------

// Ejercicio 20: Calcular el Porcentaje de Avistamientos con Secuestros
// Usa filter y foldLeft para calcular el porcentaje de avistamientos que incluyen
// eventos de secuestro.

val avistamientos = df.filter(col("Abduction event") === "Yes")
val avistamientsoLista = avistamientos.collect().toList
val totalFilas = df.collect().toList
val totalAvistamientos = avistamientsoLista.foldLeft(0)( (contador,_) => contador + 1)
val contarTotalFilas = totalFilas.foldLeft(0)( (contador, _) => contador + 1 )
val porcentajeAvistamientos = (totalAvistamientos * 100)/contarTotalFilas

// COMMAND ----------



// COMMAND ----------

print("Porcentaje avistamientos: " + porcentajeAvistamientos + " %")

// COMMAND ----------

// Ejercicio 21: Ordenar Avistamientos por Latitud y Longitud
// Usa sortBy para ordenar los avistamientos primero por latitud y luego por longitud en
// orden ascendente.


val dfSorted = df.orderBy(col("latitude").asc, col("longitude").asc)


dfSorted.show()

// COMMAND ----------

// Ejercicio 22: Filtrar Avistamientos con Incidentes de Vacas y Secuestros
// Usa filter para obtener una lista de avistamientos que reportaron tanto incidentes de
// vacas como eventos de secuestro


val avistamientosVacasSecuestros = df.filter( col("Cow incidents") === "Yes" && col("Abduction event") === "Yes")



avistamientosVacasSecuestros.show()

// COMMAND ----------

     val circuloYearInput = df.filter(col("Crop circle found") === "Yes" && col("year") === 1712)
      val circuloYearInputLista = cultivoYear.collect().toList
      val totalCirculosYear = circuloYearInputLista.foldLeft(0)( (contador, _) => contador + 1 )

// COMMAND ----------

// Ejercicio 23: Calcular el Número Total de Círculos de Cultivo en un Año Específico
// Usa filter y foldLeft para calcular el número total de círculos de cultivo
// encontrados en un año específico

def sacarCultivoYear (year: Int) : Int = {


      val circuloYearInput = df.filter(col("Crop circle found") === "Yes" && col("year") === year)
      val circuloYearInputLista = cultivoYear.collect().toList
      val totalCirculosYear = circuloYearInputLista.foldLeft(0)( (contador, _) => contador + 1 )
  
      totalCirculosYear

}

val numCultivosY = sacarCultivoYear(1712)

// COMMAND ----------

// Ejercicio 24: Contar el Número de Avistamientos por Coordenadas
// Usa groupBy y map para contar el número de avistamientos por coordenadas
// (combinación de latitud y longitud).
import org.apache.spark.sql.types.IntegerType


//como por cada registro salen coordenadas unicas, vamos a castear las coordenadas a entero
val coordenadasAvistamientosRaw = df .withColumn("longitudeInt", col("longitude").cast(IntegerType) )
                                     .withColumn("latitudeInt", col("latitude").cast(IntegerType) )
                                     .filter(col("Alien sighted") === "Yes")
                                     .groupBy("longitudeInt", "latitudeInt").count()

coordenadasAvistamientosRaw
                         
                          .show()





// COMMAND ----------

// Ejercicio 25: Encontrar el Año con Más Incidentes de Vacas
// Usa groupBy y foldLeft para encontrar el año con el mayor número de incidentes de
// vacas


val vacasYear = df.filter(col("`Cow incidents`") === "Yes").groupBy("Cow incidents", "year").count()
val vacasYearList = vacasYear.collect().toList

val totalVacasYearList = vacasYearList.map(fila => fila.getAs[Long]("count")).foldLeft(0.0)(_ max _)

val mayorVacaYear = vacasYear.filter(col("count") === totalVacasYearList)

mayorVacaYear.show()



// COMMAND ----------

// Ejercicio 26: Calcular el Promedio de Avistamientos con Aliens por Año
// Usa groupBy y map para calcular el promedio de avistamientos con aliens por año.


val aliensYear = df.filter(col("`Alien sighted`") === "Yes").groupBy("Alien sighted", "year").count()
val aliensYearLista = aliensYear.collect().toList
val totalAliensYear = aliensYearLista.map(fila => fila.getAs[Long]("count")).foldLeft(0.0)(_ + _)
val promedioAliensYear = totalAliensYear / aliensYearLista.length

// COMMAND ----------

// Ejercicio 28: Contar Avistamientos con Círculos de Cultivo y Secuestros
// Usa filter para contar el número de avistamientos que reportaron tanto círculos de
// cultivo como eventos de secuestro.

val avistamientosCultivosSecuestros = df.filter(col("Crop circle found") === "Yes" && col("Abduction event") === "Yes").count()

// COMMAND ----------

// Ejercicio 29: Calcular el Promedio de Incidentes de Vacas en una Longitud
// Específica
// Usa filter y foldLeft para calcular el promedio de incidentes de vacas en una
// longitud específica.


def promedioLongitudVacas (longitud: Double): Double = {

  val vacasIncidentes = df.filter(col("Cow incidents") === "Yes" && col("longitude") ===  longitud)
  val vacasIncidentesListas = vacasIncidentes.collect().toList
  val totalFilas = df.collect().toList.length.toDouble
  val totalVacasIncidentes = vacasIncidentesListas.foldLeft(0)( (cont, _) => cont + 1).toDouble

  val promedioIncidentesVacasLongitud = totalVacasIncidentes / totalFilas
  promedioIncidentesVacasLongitud

}


val longitudVaca = promedioLongitudVacas(-3.3403758389564153)





// COMMAND ----------

// Ejercicio 30: Mostrar Avistamientos en una Coordenada Específica
// Usa filter para obtener una lista de avistamientos que ocurrieron en una coordenada
// específica (combinación de latitud y longitud).

def mostrarAvistamientos (longitud: Double, latitud: Double) : DataFrame = {

  val avistamientoCoordenada = df.filter(
                                        col("Alien sighted") === "Yes" &&
                                        col("longitude") === longitud &&
                                        col("latitude") === latitud
                                        )
      avistamientoCoordenada
}


val avistamiento = mostrarAvistamientos(-2.479345704888089,37.844380116816964 )


// COMMAND ----------

// Ejercicio 31: Contar el Número de Avistamientos por Año y Coordenadas
// Usa groupBy y map para contar el número de avistamientos por año y coordenadas.



df.withColumn("longitudeInt", col("longitude").cast(IntegerType) )
                                     .withColumn("latitudeInt", col("latitude").cast(IntegerType) )
                                     .filter(col("Alien sighted") === "Yes")
                                     .groupBy("longitudeInt", "latitudeInt", "year").count().orderBy("count").show()

// COMMAND ----------

dfSecuestroYear.printSchema

// COMMAND ----------

// Ejercicio 32: Encontrar el Año con Más Secuestros
// Usa groupBy y foldLeft para encontrar el año con el mayor número de eventos de
// secuestro.

val dfSecuestroYear = df.filter(col("`Abduction event`") === "Yes").groupBy("Abduction event", "year").count()
val secuestroYearLista = dfSecuestroYear.collect().toList
val secuestroMax = secuestroYearLista.map(fila => fila.getAs[Long]("count")).foldLeft(0.0)(_ max _)
val dfSacaMaxYear = dfSecuestroYear.filter (col("count") === secuestroMax)

dfSacaMaxYear.show()




// COMMAND ----------

// Ejercicio 33: Calcular el Promedio de Avistamientos por Año en una Longitud Específica
// Usa filter, groupBy y map para calcular el promedio de avistamientos por año en una
// longitud específica.


def sacaPromedio (longitud: Int) : Double = {
val promedioAvistamientos =  df.withColumn("longitudeInt", col("longitude").cast(IntegerType) )
                            .filter(col("Abduction event") === "Yes" && col("longitudeInt") ===  longitud)
                            .groupBy("Abduction event", "year", "longitudeInt")
                            .count()
                            

val promedioAvistamientosLista = promedioAvistamientos.collect().toList
val calcularPromedioLista = promedioAvistamientosLista.map( fila => fila.getAs[Long]("count") ).foldLeft(0.0)(_+_)
val promedioAvistamientosCalculado = calcularPromedioLista / promedioAvistamientosLista.length
promedioAvistamientosCalculado
} 

val promedioPruebaYear = sacaPromedio(-2)


// COMMAND ----------

// Ejercicio 34: Filtrar Avistamientos con Aliens y Secuestros
// Usa filter para obtener una lista de avistamientos que reportaron tanto
// avistamientos de aliens como eventos de secuestro.
//|Alien sighted|Abduction event|
val alienNSecuentos = df.filter (col("`Alien sighted`") === "Yes" && col("`Abduction event`") === "Yes")


alienNSecuentos.show()


// COMMAND ----------

// Ejercicio 35: Contar el Número Total de Avistamientos en un Año Específico
// Usa filter y foldLeft para contar el número total de avistamientos en un año
// específico.


//Definidmos funcion que nos devuelva el nº de avistamientos a partir de un año especifico

def cuentaAvistamientosYear (anio: Integer) : Integer = {

  val avistamientosYear = df.filter(col("`Alien sighted`") === "Yes"  && col("year") === anio )
  val avistamientosYearLista = avistamientosYear.collect().toList
  val cuentaAvistamientosYear = avistamientosYearLista.foldLeft(0)( (contador, _) => contador + 1  )
  cuentaAvistamientosYear
}


val anio1846 = cuentaAvistamientosYear(1846)

// COMMAND ----------



// Ejercicio 36: Encontrar la Latitud con Más Círculos de Cultivo
// Usa groupBy y foldLeft para encontrar la latitud con el mayor número de círculos de
// cultivo encontrados.

//Crop circle found

val latitudesCultivos = df.withColumn("latitudeInt", col("latitude").cast(IntegerType))
                          .filter( col("`Crop circle found`") === "Yes" )
                          .groupBy("Crop circle found", "latitudeInt")
                          .count()

val latitudesCultivosLista = latitudesCultivos.collect().toList

// .map(fila => fila.getAs[Long]("count")).foldLeft(0.0)(_ max _)
val sacaMaxLatitudes = latitudesCultivosLista.map(fila => fila.getAs[Long]("count")).foldLeft(0.0)(_ max _)

val filtrarLatitudCultivo = latitudesCultivos.filter (col("count") === sacaMaxLatitudes.toInt)

filtrarLatitudCultivo.show()

// COMMAND ----------

// Ejercicio 37: Calcular el Porcentaje de Avistamientos con Incidentes de Vacas
// Usa filter y foldLeft para calcular el porcentaje de avistamientos que incluyen
// incidentes de vacas.

val vacasIncidente = df.filter(col("`Cow incidents`") === "Yes")

val vacasIncidenteLista = vacasIncidente.collect().toList
val total = df.collect().toList.length
val avistamientosVacas = vacasIncidenteLista.foldLeft(0)( (contador,_) => contador + 1)



val porcentajeAvistamientos = (avistamientosVacas.toDouble / total.toDouble) * 100


print("Porcentaje de avistamientos con incidentes de vacas: " + porcentajeAvistamientos)

// COMMAND ----------

// Ejercicio 38: Ordenar Avistamientos por Año y Coordenadas
// Usa sortBy para ordenar los avistamientos primero por año y luego por coordenadas
// en orden ascendente.

val avistamientosOrdenadosYearCoord =  df.filter( col("`Alien sighted`") === "Yes").orderBy("year", "longitude", "latitude")


avistamientosOrdenadosYearCoord.show()


// COMMAND ----------

// Ejercicio 39: Filtrar Avistamientos en un Rango de Longitudes
// Usa filter para obtener una lista de avistamientos que ocurrieron en un rango
// específico de longitudes.

val longitudRaw = df.withColumn("longitudeeInt", col("longitude").cast(IntegerType))



val longitudRango = longitudRaw.filter( col("`Alien sighted`") === "Yes" && col("longitudeeInt") > -5 &&  col("longitudeeInt")< 1)

longitudRango.show()

// COMMAND ----------

// Ejercicio 40: Calcular el Promedio de Secuestros en una Latitud Específica
// Usa filter y foldLeft para calcular el promedio de eventos de secuestro en una
// latitud específica.


def promedioLatitud (latitud: Int) : Double = {
val latitudRaw = df.withColumn("latitudeInt", col("latitude").cast(IntegerType))
                    .filter(col("`Abduction event`") === "Yes" && col("latitudeInt") === latitud)

val todoSecuestros = df.filter(col("`Abduction event`") === "Yes")


val latitudRawLista = latitudRaw.collect().toList
val secuestrosListaTotal = todoSecuestros.collect().toList.size

val totalLatitud = latitudRawLista.foldLeft(0)( (contador, _) => contador + 1 )


val promedio = totalLatitud.toDouble / secuestrosListaTotal.toDouble
promedio
}

val latitudPrueba = promedioLatitud(42)