package com.example
package preprocessing

import readers.CsvReader

import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger

import java.sql.Date

trait DateCheck {
  def check(config: CsvReader.Config): (Date, Boolean)
}
