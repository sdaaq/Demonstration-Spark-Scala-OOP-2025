package com.example
package metrics


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.example.transformers._

class TopAirportsAnalyzer extends TopAirports
{
  override def getTop(
              flightsDF: DataFrame,
              airportsDF: DataFrame,
              aggDF: DataFrame,
              topCount: Integer,
              order: String)
              :(DataFrame, DataFrame, DataFrame) = {

    val isNotCancelled =  col("cancelled") === 0
    val byAirportCode = col("originAirport") === col("iataCode")

    val filteredDF = flightsDF
      .transform(FilterProducer.filter(isNotCancelled))

    val originAirportsDF = filteredDF
      .transform(ColumnSelector.select(col("originAirport")))

    val destinationAirportsDF = filteredDF
      .transform(ColumnSelector.select(col("destinationAirport")))

    val topAirportsCountDF = originAirportsDF
      .transform(Union.union(destinationAirportsDF))
      .transform(ColumnCounter.count(col("originAirport")))

    val aggTopAirports = {
      if (aggDF.take(1).isEmpty) {
        topAirportsCountDF
      }
      else {
        aggDF
          .transform(Union.union(topAirportsCountDF))
          .transform(ColumnSummer.sumColumns(col("count"), col("originAirport")))

      }

    }

    val topAirportsDF = aggTopAirports
      .transform(DataframeJoiner.join(airportsDF, byAirportCode, "left"))
      .transform(ColumnSorter.sort(col("count"), order))
      .transform(RowLimiter.limit(topCount))
      .transform(ColumnSelector.select(airportsDF.col("airport"), col("count")))


    (filteredDF, topAirportsDF, aggTopAirports)
  }
}
