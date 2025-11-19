package com.example

import org.apache.spark.sql.SparkSession
import com.example.config.Config

trait SessionWrapper {
  val appName: String

  lazy val spark = createSession(appName)

  private def createSession(appName: String) = {

    SparkSession.builder()
      .appName(appName)
      .master(Config.getKey("master"))
      .getOrCreate()

  }
}


