package com.example
package transformers

import org.apache.spark.sql.{Column, DataFrame}

object WithSpecifiedColumn {
  def withCol(columnName: String, column: Column)(df: DataFrame): DataFrame = {
    df
      .withColumn(columnName, column)
  }

}
