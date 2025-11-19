package com.example
package schemas

import org.apache.spark.sql.types._

object Airports {
  val schema: StructType = StructType(Seq(
    StructField("iataCode", StringType),
    StructField("airport", StringType),
    StructField("city", StringType),
    StructField("state", StringType),
    StructField("country", StringType),
    StructField("latitude", StringType),
    StructField("longitude", StringType),
  ))

}
