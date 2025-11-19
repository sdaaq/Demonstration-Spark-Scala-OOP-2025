package com.example
package transformers

import org.apache.spark.sql.DataFrame

object Union {
  def union(rightDF: DataFrame)(leftDF: DataFrame): DataFrame = {
    leftDF
      .union(rightDF)
  }

}
