package com.example
package transformers

import org.apache.spark.sql.{Column, DataFrame}

object ColumnSelector {
  def select(cols: Column*)(df: DataFrame): DataFrame = {
    df.select(cols: _*)
  }
}
