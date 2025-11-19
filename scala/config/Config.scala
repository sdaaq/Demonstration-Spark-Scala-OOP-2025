package com.example
package config

object Config {
  private val dev: Map[String, String] = {
    Map("master" -> "local", //Запуск в ide
        "storagePath" -> "storage_dir", //Папка для хранения данных при запуске в ide (Можно задать в аргументах spark-submit)
        "rootDir" -> "")  //Папка для хранения метаданных при запуске в ide (Можно задать в аргументах spark-submit)
  }

  private val prod: Map[String, String] = {
    Map("master" -> "spark://spark:7077", //Запуск на кластере
        "storagePath" -> "/opt/spark-data/storage_dir", //Папка для хранения данных при запуске в докере (Можно задать в аргументах spark-submit)
        "rootDir" -> "/opt/spark-data/") // Папка для хранения метаданных при запуске в докере (Можно задать в аргументах spark-submit)
  }

  //При запуске на кластере подставляются другие пути по умолчанию.
  //Переменные окружения: SESSION_ENV=dev;PATH_ENV=data_source

  private val environmentStart = sys.env.getOrElse("SESSION_ENV", "prod") //Переменная окружения для запуская в IDE
  private val environmentPath = sys.env.get("PATH_ENV") //Переменная окружения для указания пути источника данных


  def getPath(path: Option[String]): String = {
    val correctPath: String = environmentPath
      .orElse(path)
      .getOrElse{"EmptyPath"}

    correctPath
  }

  def getKey(key: String): String = {
    environmentStart match {
      case "dev" => dev(key)
      case _ => prod(key)
    }

  }
}