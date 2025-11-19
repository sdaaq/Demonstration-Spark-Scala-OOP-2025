package com.example
package metrics

import org.apache.spark.sql.DataFrame

trait TopDayOfWeek {
  def getTop(df: DataFrame,
             aggDF: DataFrame,
             order: String = "desc") //Значение по умолчанию
             : (DataFrame, DataFrame)


}
