package com.example
package schemas

import org.apache.spark.sql.types._

object AggDelayReasons {
  val schema: StructType = StructType(Seq(
    StructField("totalDelay", IntegerType),
    StructField("airSystemDelay", IntegerType),
    StructField("securityDelay", IntegerType),
    StructField("airlineDelay", IntegerType),
    StructField("lateAircraftDelay", IntegerType),
    StructField("weatherDelay", IntegerType),
    StructField("airSystemDelayCnt", IntegerType),
    StructField("securityDelayCnt", IntegerType),
    StructField("airlineDelayCnt", IntegerType),
    StructField("lateAircraftDelayCnt", IntegerType),
    StructField("weatherDelayCnt", IntegerType),


  ))

}