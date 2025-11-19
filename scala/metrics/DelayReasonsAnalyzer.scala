package com.example
package metrics

import com.example.transformers.{Union, ColumnSelector}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame


class DelayReasonsAnalyzer(
                    df: DataFrame,
                    aggDF: DataFrame) extends DelayReasons {
  private val delaySumCountDF: DataFrame = df
    .transform(ColumnSelector.select(
      sum(when(col("arrivalDelay") > 0, col("arrivalDelay")).otherwise(0)).as("totalDelay"),
      sum(when(col("airSystemDelay") > 0, col("airSystemDelay")).otherwise(0)).as("airSystemDelay"),
      sum(when(col("securityDelay") > 0, col("securityDelay")).otherwise(0)).as("securityDelay"),
      sum(when(col("airlineDelay") > 0, col("airlineDelay")).otherwise(0)).as("airlineDelay"),
      sum(when(col("lateAircraftDelay") > 0, col("lateAircraftDelay")).otherwise(0)).as("lateAircraftDelay"),
      sum(when(col("weatherDelay") > 0, col("weatherDelay")).otherwise(0)).as("weatherDelay"),
      sum(when(col("airSystemDelay") > 0, 1).otherwise(0)).as("airSystemDelayCnt"),
      sum(when(col("securityDelay") > 0, 1).otherwise(0)).as("securityDelayCnt"),
      sum(when(col("airlineDelay") > 0, 1).otherwise(0)).as("airlineDelayCnt"),
      sum(when(col("lateAircraftDelay") > 0, 1).otherwise(0)).as("lateAircraftDelayCnt"),
      sum(when(col("weatherDelay") > 0, 1).otherwise(0)).as("weatherDelayCnt"))
    )

  val aggDelaySumCountDF: DataFrame = {
    if (aggDF.take(1).isEmpty) {
      delaySumCountDF
    }
    else {
      aggDF
        .transform(Union.union(delaySumCountDF))
        .transform(ColumnSelector.select(
          sum(col("totalDelay")).as("totalDelay"),
          sum(col("airSystemDelay")).as("airSystemDelay"),
          sum(col("securityDelay")).as("securityDelay"),
          sum(col("airlineDelay")).as("airlineDelay"),
          sum(col("lateAircraftDelay")).as("lateAircraftDelay"),
          sum(col("weatherDelay")).as("weatherDelay"),
          sum(col("airSystemDelayCnt")).as("airSystemDelayCnt"),
          sum(col("securityDelayCnt")).as("securityDelayCnt"),
          sum(col("airlineDelayCnt")).as("airlineDelayCnt"),
          sum(col("lateAircraftDelayCnt")).as("lateAircraftDelayCnt"),
          sum(col("weatherDelayCnt")).as("weatherDelayCnt"))
        )
    }
  }


  override def countFlights: DataFrame = {
    val delayFlightsCountDF = aggDelaySumCountDF
      .transform(ColumnSelector.select(
        col("airSystemDelayCnt"),
        col("securityDelayCnt"),
        col("airlineDelayCnt"),
        col("lateAircraftDelayCnt"),
        col("weatherDelayCnt"))
      )

    delayFlightsCountDF
  }


  override def calcPercentTimeDelay: DataFrame = {
    val percentageDelayDF = aggDelaySumCountDF
      .transform(ColumnSelector.select(round(col("airSystemDelay") / col("totalDelay") * 100, 2).as("airSystemDelay"),
        round(col("securityDelay") / col("totalDelay") * 100, 2).as("securityDelay"),
        round(col("airlineDelay") / col("totalDelay") * 100, 2).as("airlineDelay"),
        round(col("lateAircraftDelay") / col("totalDelay") * 100, 2).as("lateAircraftDelay"),
        round(col("weatherDelay") / col("totalDelay") * 100, 2).as("weatherDelay")))

    percentageDelayDF
  }

}
