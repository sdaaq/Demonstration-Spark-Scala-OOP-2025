package com.example
package writers

import org.apache.spark.sql.{DataFrame, SaveMode}

object CsvWriter {
  final case class Config(
                     path: String,
                     separator: Char = ',',
                     hasHeader: Boolean = true,
                     saveMode: SaveMode = SaveMode.Overwrite
                   )
}

class CsvWriter extends Writer[CsvWriter.Config]{
  override def write(df: DataFrame, config: CsvWriter.Config): Unit = {
    df.write
      .mode(config.saveMode)
      .option("sep", config.separator.toString)
      .option("header", config.hasHeader.toString.toLowerCase)
      .csv(config.path)
  }


}
