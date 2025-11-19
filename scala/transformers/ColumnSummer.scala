package com.example
package transformers

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.sum

object ColumnSummer {

  def sumColumns(aggCol: Column, cols: Column*)(df: DataFrame): DataFrame = {
    df
      .groupBy(cols: _*)
      .agg(sum(aggCol).as("count"))
  }

}