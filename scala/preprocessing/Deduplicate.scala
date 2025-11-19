package com.example
package preprocessing

import org.apache.spark.sql.DataFrame

trait Deduplicate {
  def deduplicate(df: DataFrame): DataFrame

}
