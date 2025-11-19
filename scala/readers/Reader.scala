package com.example
package readers

import org.apache.spark.sql.DataFrame

trait Reader[Config] {
  def read(config: Config): DataFrame
}
