package com.example
package schemas

import org.apache.spark.sql.types._

object Flights {
  val schema: StructType = StructType(Seq(
    StructField("year", IntegerType),
    StructField("month", IntegerType),
    StructField("day", IntegerType),
    StructField("dayOfWeek", IntegerType),
    StructField("airline", StringType),
    StructField("flightNumber", StringType),
    StructField("tailNumber", StringType),
    StructField("originAirport", StringType),
    StructField("destinationAirport", StringType),
    StructField("scheduledAirport", IntegerType),
    StructField("departureTime", IntegerType),
    StructField("departureDelay", IntegerType),
    StructField("taxiOut", IntegerType),
    StructField("wheelsOff", IntegerType),
    StructField("scheduledTime", StringType),
    StructField("elapsedAirTime", StringType),
    StructField("airTime", StringType),
    StructField("airDistance", StringType),
    StructField("wheelsOn", IntegerType),
    StructField("taxiIn", IntegerType),
    StructField("scheduledArrival", IntegerType),
    StructField("arrivalTime", IntegerType),
    StructField("arrivalDelay", IntegerType),
    StructField("diverted", IntegerType),
    StructField("cancelled", IntegerType),
    StructField("cancellationReason", StringType),
    StructField("airSystemDelay", IntegerType),
    StructField("securityDelay", IntegerType),
    StructField("airlineDelay", IntegerType),
    StructField("lateAircraftDelay", IntegerType),
    StructField("weatherDelay", IntegerType)
  ))
}
