package com.example
package readers

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object CsvReader {
  final case class Config(file: String, scheme: StructType, separator: Char = ',', hasHeader: Boolean = true)
}

class CsvReader(spark: SparkSession) extends Reader[CsvReader.Config] {
  override def read(config: CsvReader.Config): DataFrame = {
    spark.read
      .option("header", config.hasHeader.toString.toLowerCase)
      .option("sep", config.separator.toString)
      .schema(config.scheme)
      .csv(config.file)
  }
}
