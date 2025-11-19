package com.example
package schemas

import org.apache.spark.sql.types._

object AggTopDestinationAirports {
  val schema: StructType = StructType(Seq(
    StructField("originAirport", StringType),
    StructField("destinationAirport", StringType),
    StructField("count", IntegerType),
  ))

}