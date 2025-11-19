package com.example
package transformers

import org.apache.spark.sql.DataFrame

object RowLimiter {
  def limit(n: Int)(df: DataFrame): DataFrame = {
    df.limit(n)
  }
}
