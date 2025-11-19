package com.example
package preprocessing

import readers.{CsvReader, Reader}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col

import java.sql.Date

class DateChecker(fs: FileSystem,
                  logger: Logger,
                  reader: Reader[CsvReader.Config]) extends DateCheck {
  override def check(config: CsvReader.Config): (Date, Boolean) = {

    val successPath = new Path(config.file, "_SUCCESS")

    if (fs.exists(successPath)) {
      logger.warn("Есть файл с меткой. В расчете будут использоваться накопленные данные")

      val collectingDate = reader
        .read(config)
        .select(col("rightBoundDate"))
        .head
        .getDate(0)

      (collectingDate, false)
    }
    else {
      logger.warn("Файла с меткой нет. Расчет будет только на новых данных")
      (Date.valueOf("0001-01-01"), true)
    }

  }
}
