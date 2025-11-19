package com.example
package metrics

import org.apache.spark.sql.functions._
import org.apache.spark.sql. DataFrame
import com.example.transformers._


class TopAirlinesAnalyzer extends TopAirlines
{

  override def getTop(
              flightsDF: DataFrame,
              airlinesDF: DataFrame,
              aggDF: DataFrame,
              topCount: Integer,
              order: String)
              :(DataFrame, DataFrame) = {

    val isCorrectDelay = col("arrivalDelay") <= 0 and col("departureDelay") <= 0

    val topAirlinesCountDF = flightsDF
      .transform(FilterProducer.filter(isCorrectDelay))
      .transform(ColumnCounter.count(col("airline")))

    val aggTopAirlinesDF = {
      if (aggDF.take(1).isEmpty) {
        topAirlinesCountDF
      }
      else {
        aggDF
          .transform(Union.union(topAirlinesCountDF))
          .transform(ColumnSummer.sumColumns(col("count"), col("airline")))
      }
    }
    val byAirlineCode = aggTopAirlinesDF.col("airline") === airlinesDF.col("iataCode")

    val topAirlinesDF = aggTopAirlinesDF
      .transform(DataframeJoiner.join(airlinesDF, byAirlineCode, "left"))
      .transform(ColumnSorter.sort(col("count"), order))
      .transform(RowLimiter.limit(topCount))
      .transform(ColumnSelector.select(airlinesDF.col("airline"), col("count")))


    (topAirlinesDF, aggTopAirlinesDF)
  }

}
