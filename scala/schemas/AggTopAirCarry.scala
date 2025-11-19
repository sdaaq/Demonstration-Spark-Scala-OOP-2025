package com.example
package schemas

import org.apache.spark.sql.types._

object AggTopAirCarry {
  val schema: StructType = StructType(Seq(
    StructField("originAirport", StringType),
    StructField("airline", StringType),
    StructField("count", IntegerType),
  ))

}