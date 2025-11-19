package com.example

import com.example.jobs.FlightAnalyzeJob
import com.example.config.Config
import com.example.config.Directory
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager



object FlightAnalyzer extends SessionWrapper{
  override val appName: String = "Airport Analyze"

  def main(args: Array[String]): Unit = {
    val logger = LogManager.getRootLogger

    try {
      val dataSourceDir: String = Config.getPath(args.headOption) //Источник данных. Всегда нужно указывать. Для запуска в IDE задается через переменную окружения "PATH_ENV"

      if (args.length > 3 || dataSourceDir == "EmptyPath") {
        println("Specify arguments to the job")
        System.exit(1)
      }

      spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val storageDir = args.lift(1).getOrElse(Config.getKey("storagePath")) // Место хранения результатов и накопленных данных. Значения по умолчанию описаны в Config
      val rootDir = args.lift(2).getOrElse(Config.getKey("rootDir")) //Корневая папка для метаданных. Значения по умолчанию описаны в Config

      val directory = Directory(
        dataSourceDir,
        storageDir,
        rootDir
      )

      val flightAnalyzeJob = new FlightAnalyzeJob(
        spark,
        fs,
        directory,
        logger,
        topCount = 10, //Выборка топа. По умолчанию стоит 10 строк
        order = "desc" //Сортировка топа(asc, desc). По умолчанию стоит по убыванию
      )

      flightAnalyzeJob.run()

      spark.stop()
    }
    catch {
      case e: Exception =>
        logger.error("Unexpected error during job execution", e)
        spark.stop()
        System.exit(1)
    }

  }
}
