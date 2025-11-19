package com.example
package schemas

import org.apache.spark.sql.types._

object AggTopAirlines {
  val schema: StructType = StructType(Seq(
    StructField("airline", StringType),
    StructField("count", IntegerType),
  ))

}