package com.example
package schemas

import org.apache.spark.sql.types._

object Airlines {
  val schema: StructType = StructType(Seq(
    StructField("iataCode", StringType),
    StructField("airline", StringType),
  ))
}
