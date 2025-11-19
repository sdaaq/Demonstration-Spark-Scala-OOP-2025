package com.example
package metrics


import org.apache.spark.sql.DataFrame

trait TopAirlines {
  def getTop(flightsDF: DataFrame,
             airportsDF: DataFrame,
             aggDF: DataFrame,
             topCount: Integer = 10, //Значение по умолчанию
             order: String = "desc") //Значение по умолчанию
  :(DataFrame, DataFrame)
}
