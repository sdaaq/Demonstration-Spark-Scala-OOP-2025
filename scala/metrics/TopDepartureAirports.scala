package com.example
package metrics

import org.apache.spark.sql.DataFrame

trait TopDepartureAirports {
  def getTopDestinationAirports(aggDF: DataFrame): (DataFrame, DataFrame)

  def getTopAirlines(aggDF: DataFrame): (DataFrame, DataFrame)
}
