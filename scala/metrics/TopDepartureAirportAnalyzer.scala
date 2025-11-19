package com.example
package metrics

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.example.transformers.{ColumnCounter, FilterProducer, WithWindowColumn, Union, ColumnSummer}
import com.example.constants.ErrorMessage

class TopDepartureAirportAnalyzer(
                              flightsDF: DataFrame,
                              topCount: Integer,
                              order: String)
extends TopDepartureAirports
{


  val orderedCol: Column = order.toLowerCase match {
    case "desc" => col("count").desc
    case "asc"  => col("count").asc
    case _      => throw new IllegalArgumentException(ErrorMessage.wrongSortingOrder(order))
  }

  private val isCorrectDelay: Column = col("departureDelay") <= 0

  private val DepartureAirportDF: DataFrame = flightsDF
    .transform(FilterProducer.filter(isCorrectDelay))


  override def getTopDestinationAirports(aggDF: DataFrame): (DataFrame, DataFrame) = {
    val windowSpec = Window
      .partitionBy("originAirport")
      .orderBy(orderedCol)

    val isValidRank = col("rank") <= topCount

    val cols = List(
      col("originAirport"),
      col("destinationAirport"),
      col("count")
    )

    val topDestinationAirportsCountDF = DepartureAirportDF
      .transform(ColumnCounter.count(col("originAirport"), col("destinationAirport")))

    val aggtopDestinationAirportsDF = {
      if (aggDF.take(1).isEmpty) {
        topDestinationAirportsCountDF
      }
      else {
        aggDF
          .transform(Union.union(topDestinationAirportsCountDF))
          .transform(ColumnSummer.sumColumns(col("count"), col("originAirport"), col("destinationAirport")))
      }
    }

    val topDestinationAirportsDF = aggtopDestinationAirportsDF
      .transform(WithWindowColumn.withRankColumn(cols: _*)(windowSpec))
      .transform(FilterProducer.filter(isValidRank))


    (topDestinationAirportsDF, aggtopDestinationAirportsDF)
  }


  override def getTopAirlines(aggDF: DataFrame): (DataFrame, DataFrame) = {
    val windowSpec = Window
      .partitionBy("originAirport")
      .orderBy(orderedCol)

    val isValidRank = col("rank") <= topCount

    val cols = List(
      col("originAirport"),
      col("airline"),
      col("count"),
    )

    val topOriginAirlinesCountDF = DepartureAirportDF
      .transform(ColumnCounter.count(col("originAirport"), col("airline")))

    val aggTopOriginAirlinesDF = {
      if (aggDF.take(1).isEmpty) {
        topOriginAirlinesCountDF
      }
      else {
        aggDF
          .transform(Union.union(topOriginAirlinesCountDF))
          .transform(ColumnSummer.sumColumns( col("count"), col("originAirport"), col("airline")))
      }
    }

    val topOriginAirlinesDF = aggTopOriginAirlinesDF
      .transform(WithWindowColumn.withRankColumn(cols: _*)(windowSpec))
      .transform(FilterProducer.filter(isValidRank))

    (topOriginAirlinesDF, aggTopOriginAirlinesDF)
  }

}
