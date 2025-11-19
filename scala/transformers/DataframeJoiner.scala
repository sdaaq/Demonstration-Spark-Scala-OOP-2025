package com.example
package transformers

import org.apache.spark.sql.{Column, DataFrame}

object DataframeJoiner {
  def join(rightDF: DataFrame, condition: Column, method: String)(leftDF: DataFrame): DataFrame = {
    leftDF
      .join(rightDF, condition, method)
  }
}
