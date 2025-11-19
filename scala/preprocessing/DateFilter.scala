package com.example
package preprocessing


import com.example.transformers.{FilterProducer, WithSpecifiedColumn}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, concat_ws, date_sub, lit, max, min, to_date}

import java.sql.Date

object DateFilter {
  private def convertToDate(): Column = {
    to_date(concat_ws("-", col("year"), col("month"), col("day")), "yyyy-M-d")
  }

  private def getDateBounds(df: DataFrame): (Date, Date) = {
    val dateBounds = df
      .select(date_sub(max(col("fullDate")), 1), min(col("fullDate")))
      .head

    (dateBounds.getDate(0), dateBounds.getDate(1))
  }
}

class DateFilter extends DateFiltration {
  override def filter(df: DataFrame, date: Date): (DataFrame, Date, Date) = {
    val dateColumn = DateFilter.convertToDate()

    val flightsWithColDateDF = df
      .transform(WithSpecifiedColumn.withCol("fullDate", dateColumn))

    val (maxDate, minDate) = (DateFilter.getDateBounds(flightsWithColDateDF))

    val hasProperDate = lit(date) < col("fullDate") && col("fullDate") < lit(maxDate)
    val isValidAirport =  !col("originAirport").rlike("^\\d+$") && !col("destinationAirport").rlike("^\\d+$")

    val filteredDF = flightsWithColDateDF
      .transform(FilterProducer.filter(hasProperDate && isValidAirport))

    (filteredDF, minDate, maxDate)
  }
}

