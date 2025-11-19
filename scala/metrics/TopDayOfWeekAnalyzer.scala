package com.example
package metrics

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.example.transformers.{ColumnCounter, FilterProducer, ColumnSorter, Union, ColumnSummer}


class TopDayOfWeekAnalyzer extends TopDayOfWeek
{
  override def getTop(df: DataFrame,
             aggDF: DataFrame,
             order: String)
             : (DataFrame, DataFrame) = {

    val isCorrectDelay =  col("arrivalDelay") <= 0

    val topDayOfWeekCountDF = df
      .transform(FilterProducer.filter(isCorrectDelay))
      .transform(ColumnCounter.count(col("dayOfWeek")))

    val aggTopDayOfWeekDF = {
      if (aggDF.take(1).isEmpty) {
        topDayOfWeekCountDF
      }
      else {
        aggDF
          .transform(Union.union(topDayOfWeekCountDF))
          .transform(ColumnSummer.sumColumns(col("count"), col("dayOfWeek")))
      }
    }

    val topDayOfWeekDF = aggTopDayOfWeekDF
      .transform(ColumnSorter.sort(col("count"), order))


    (topDayOfWeekDF, aggTopDayOfWeekDF)
  }

}
