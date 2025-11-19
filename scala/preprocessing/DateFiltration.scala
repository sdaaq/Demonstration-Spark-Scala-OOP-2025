package com.example
package preprocessing

import org.apache.spark.sql.DataFrame

import java.sql.Date

trait DateFiltration {
  def filter(df: DataFrame, date: Date): (DataFrame, Date, Date)
}
