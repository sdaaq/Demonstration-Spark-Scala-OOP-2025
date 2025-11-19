package com.example
package transformers

import org.apache.spark.sql.{Column, DataFrame}

object ColumnSorter {

  def sort(col: Column, order: String)(df: DataFrame): DataFrame = {
    val orderedCol = order.toLowerCase match {
      case "desc" => col.desc
      case "asc"  => col.asc
      case _      => throw new IllegalArgumentException(s"Order must be 'asc' or 'desc', got: $order")
    }

    df
      .sort(orderedCol)
  }

}
