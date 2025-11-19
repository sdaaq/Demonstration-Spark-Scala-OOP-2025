package com.example
package transformers

import org.apache.spark.sql.{Column, DataFrame}

object ColumnCounter {

  def count(cols: Column*)(df: DataFrame): DataFrame = {
    df
      .groupBy(cols: _*)
      .count()
  }

}
