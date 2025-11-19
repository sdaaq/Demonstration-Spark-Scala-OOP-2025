package com.example
package schemas

import org.apache.spark.sql.types._

object AggDayOfWeek {
  val schema: StructType = StructType(Seq(
    StructField("dayOfWeek", StringType),
    StructField("count", IntegerType),
  ))

}