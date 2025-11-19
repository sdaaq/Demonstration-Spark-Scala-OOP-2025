
package com.example
package schemas

import org.apache.spark.sql.types._

object AggTopAirports {
  val schema: StructType = StructType(Seq(
    StructField("originAirport", StringType),
    StructField("count", IntegerType),
  ))

}