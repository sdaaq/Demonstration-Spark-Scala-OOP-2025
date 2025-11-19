package com.example
package transformers

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions._

object WithWindowColumn {
  def withRankColumn(keyColumns: Column*)(windowSpec: WindowSpec)(df: DataFrame): DataFrame = {
    df
      .select(
        keyColumns :+
        rank().over(windowSpec).as("rank"): _*
      )
  }

}
