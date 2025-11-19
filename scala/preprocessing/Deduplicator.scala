package com.example
package preprocessing

import org.apache.spark.sql.DataFrame

class Deduplicator extends Deduplicate {

  override def deduplicate(df: DataFrame): DataFrame = {
    df
      .distinct()
  }

}
