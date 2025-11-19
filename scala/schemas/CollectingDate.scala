package com.example
package schemas

import org.apache.spark.sql.types._

object CollectingDate {
  val schema: StructType = StructType(Seq(
    StructField("leftBoundDate", DateType),
    StructField("rightBoundDate", DateType),
  ))

}