package com.example
package metrics

import org.apache.spark.sql.DataFrame

trait DelayReasons {
  def countFlights: DataFrame
  def calcPercentTimeDelay: DataFrame
}
