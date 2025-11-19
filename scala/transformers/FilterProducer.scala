package com.example
package transformers

import org.apache.spark.sql.{Column, DataFrame}

object FilterProducer {
  def filter(condition: Column)(df: DataFrame): DataFrame = {
    df
      .filter(condition)
  }

}
