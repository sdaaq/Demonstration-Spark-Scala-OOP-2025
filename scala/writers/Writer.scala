package com.example
package writers

import org.apache.spark.sql.DataFrame

trait Writer[Config] {
  def write(df: DataFrame, config: Config): Unit
}
