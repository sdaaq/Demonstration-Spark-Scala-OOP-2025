package com.example
package jobs

import com.example.other._
import com.example.config.{OutputPaths, Directory}
import com.example.readers.CsvReader
import com.example.schemas._
import com.example.preprocessing._
import com.example.metrics._
import com.example.writers.CsvWriter
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.log4j.Logger



class FlightAnalyzeJob(spark: SparkSession,
                       fs: FileSystem,
                       directory: Directory,
                       logger: Logger,
                       topCount: Integer = 10, //Значение по умолчанию
                       order: String = "desc" //Значение по умолчанию
                      ) extends Job {

  override def run(): Unit = {
    //По умолчанию метрики настроены на топ - 10 по убыванию. (order = "desc") и (topCount = 10)


    import spark.implicits._

    //1) Чтение файлов

      //1.1 Чтение файлов из источника
      val reader = new CsvReader(spark)
      val dataSource = directory.dataSource

      val flightConfig = CsvReader.Config(dataSource + "/flights.csv", Flights.schema)
      val airlinesConfig = CsvReader.Config(dataSource + "/airlines.csv", Airlines.schema)
      val airportsConfig = CsvReader.Config(dataSource + "/airports.csv", Airports.schema)

      val flightsDF = reader.read(flightConfig)
      val airlinesDF = reader.read(airlinesConfig).cache()
      val airportsDF = reader.read(airportsConfig).cache()

      //1.2 Чтение накопленных агреггированных данных, если они есть

        //1.2.1 Чтение метки.
        val storageDir = directory.storageDir

        val dateSourceDir = storageDir + "/collectingDate"
        val collectingDateConfig = CsvReader.Config(dateSourceDir, CollectingDate.schema)
        val dateChecker = new DateChecker(fs, logger, reader)

        val (lastDateMeta, isEmpty) = dateChecker.check(collectingDateConfig) //Проверяет наличие метки и достает её

        //1.2.2 Чтение файлов
        val emptyDF =  Seq.empty[String].toDF("dummy")
        val aggSourceDir = storageDir + "/aggoutput"

        val aggTopAirportsConfig = CsvReader.Config(aggSourceDir +"/top_airports", AggTopAirports.schema)
        val aggTopAirlinesConfig = CsvReader.Config(aggSourceDir +"/top_airlines", AggTopAirlines.schema)
        val aggTopDestinationAirportsConfig = CsvReader.Config(aggSourceDir + "/top_destination_airport", AggTopDestinationAirports.schema)
        val aggTopAirCarryConfig = CsvReader.Config(aggSourceDir + "/topAirCarry", AggTopAirCarry.schema)
        val aggTopDayOfWeekConfig = CsvReader.Config(aggSourceDir + "/top_DayOfWeek", AggDayOfWeek.schema)
        val aggDelayReasonsConfig = CsvReader.Config(aggSourceDir + "/flights_count",AggDelayReasons.schema)

        val aggTopAirportsDF =  if (!isEmpty) {reader.read(aggTopAirportsConfig).cache()} else emptyDF
        val aggTopAirlinesDF =  if (!isEmpty) {reader.read(aggTopAirlinesConfig).cache()} else emptyDF
        val aggTopDestinationAirportsDF =  if (!isEmpty) {reader.read(aggTopDestinationAirportsConfig).cache()} else emptyDF
        val aggTopAirCarryDF =  if (!isEmpty) {reader.read(aggTopAirCarryConfig).cache()} else emptyDF
        val aggTopDayOfWeekDF =  if (!isEmpty) {reader.read(aggTopDayOfWeekConfig).cache()} else emptyDF
        val aggDelayReasonsDF =  if (!isEmpty) {reader.read(aggDelayReasonsConfig).cache()} else emptyDF


    //2) Предобработка данных

      //2.1 Фильтрация данных
      val dateFilter = new DateFilter

      val (filteredDF, leftBoundDate, rightBoundDate)  = dateFilter.filter(flightsDF, lastDateMeta)

      //2.2 Удаление дублей
      val deduplicator = new Deduplicator
      val uniqueFlightsDF = deduplicator.deduplicate(filteredDF)
      val uniqueAirlinesDF = deduplicator.deduplicate(airlinesDF)
      val uniqueAirportsDF = deduplicator.deduplicate(airportsDF)


    //3) Расчеты метрик

      //3.1 Топ-10 аэропортов по количеству совершаемых полетов.

      val topAirportsAnalyzer = new TopAirportsAnalyzer
      val (notCancelledFlightsDF, topAirportsDF, accTopAirportsDF) = topAirportsAnalyzer.getTop(uniqueFlightsDF, uniqueAirportsDF, aggTopAirportsDF, topCount, order)

      //3.2 Топ-10 авиакомпаний, вовремя выполняющих рейсы.

      val topAirlinesAnalyzer = new TopAirlinesAnalyzer
      val (topAirlinesDF, accTopAirlinesDF) = topAirlinesAnalyzer.getTop(notCancelledFlightsDF, uniqueAirlinesDF, aggTopAirlinesDF, topCount, order)

      //3.3 Топ-10 перевозчиков и аэропортов назначения на основании вовремя совершенного вылета для каждого аэропорта вылета.

      val topDepartureAirportCls = new TopDepartureAirportAnalyzer(notCancelledFlightsDF, topCount, order)

        //3.3.1 Топ-10 аэропортов назначения

        val (topDestinationAirportDF, accTopDestinationAirportDF) = topDepartureAirportCls.getTopDestinationAirports(aggTopDestinationAirportsDF)

        //3.3.2 Топ-10 перевозчиков

        val (topAirCarryDF, accTopCarryDF) = topDepartureAirportCls.getTopAirlines(aggTopAirCarryDF)

      //3.4 Дни недели в порядке своевременности прибытия рейсов, совершаемых в эти дни.

      val topDayOfWeek = new TopDayOfWeekAnalyzer

      val (topDayOfWeekDF, accTopDayOfWeekDF) = topDayOfWeek.getTop(notCancelledFlightsDF, aggTopDayOfWeekDF, order)

      //3.5 Количество рейсов, задержанных по причине: AIR_SYSTEM_DELAY / SECURITY_DELAY / AIRLINE_DELAY / LATE_AIRCRAFT_DELAY / WEATHER_DELAY

      val delayReasonsCls = new DelayReasonsAnalyzer(uniqueFlightsDF, aggDelayReasonsDF)

      val accDelaySumCountDF = delayReasonsCls.aggDelaySumCountDF

      val flightsCountDF = delayReasonsCls.countFlights

      //3.6 Процент от общего количества минут задержки рейсов для каждой причины (AIR_SYSTEM_DELAY / SECURITY_DELAY / AIRLINE_DELAY / LATE_AIRCRAFT_DELAY / WEATHER_DELAY)

      val timeDelayPercentageDF = delayReasonsCls.calcPercentTimeDelay


    //4) Запись в файлы общих результатов
    val writer = new CsvWriter

    val tmpStorageDir = storageDir + "_tmp"
    val tmpOutputDir = tmpStorageDir + "/output"
    val tmpAggOutputDir = tmpStorageDir + "/aggoutput"
    val tmpCollectingDateDir = tmpStorageDir + "/collectingDate"

    try {
      val cleaner = new DirectoriesCleaner(fs)

      cleaner.cleanUp(Seq(tmpStorageDir))

      val outDir = OutputPaths.default(tmpOutputDir)

      val outputs = Seq(
        topAirportsDF -> outDir.topAirports,
        topAirlinesDF -> outDir.topAirlines,
        topDestinationAirportDF -> outDir.topDestinations,
        topAirCarryDF -> outDir.topAirCarry,
        topDayOfWeekDF -> outDir.topDays,
        flightsCountDF -> outDir.delayCounts,
        timeDelayPercentageDF -> outDir.delayPercentages
      )

      outputs.foreach { case (df, path) =>
        writer.write(df, CsvWriter.Config(path))
      }

      logger.warn("Результаты анализа записаны во временную папку")


      //5) Запись аггрегированных данных
      val outAggDir = OutputPaths.default(tmpAggOutputDir)

      val accOutputs = Seq(
        accTopAirportsDF -> outAggDir.topAirports,
        accTopAirlinesDF -> outAggDir.topAirlines,
        accTopDestinationAirportDF -> outAggDir.topDestinations,
        accTopCarryDF -> outAggDir.topAirCarry,
        accTopDayOfWeekDF -> outAggDir.topDays,
        accDelaySumCountDF -> outAggDir.delayCounts,
      )

      accOutputs.foreach { case (df, path) =>
        writer.write(df, CsvWriter.Config(path))
      }

      logger.warn("Агреггированные данные записаны во временную папку")


      //6) Запись, временного периода, обработанных данных
      val collectingDateDF = Seq((leftBoundDate, rightBoundDate)).toDF("leftBoundDate", "rightBoundDate")

      writer.write(collectingDateDF, CsvWriter.Config(tmpCollectingDateDir))
      logger.warn("Временная метка обновлена")

      //7) Перемещение данных из временных директорий(с заменой)
      val dirMover = new DirectoriesMover(fs)
      dirMover.move(tmpStorageDir, storageDir)

      //8) Запись технической информации
      val rootDir = directory.rootDir

      val infoDF = collectingDateDF
        .select(concat_ws("-", col("leftBoundDate"), col("rightBoundDate")).as("collected"),
          current_date().as("processed"))

      writer.write(infoDF, CsvWriter.Config(rootDir + "meta_info" , saveMode = SaveMode.Append))
      logger.warn("Обновлены метаданные")


      logger.warn("Анализ выполнен успешно")
    }

    catch {
      case e: Exception =>
        //Если задача упадет, на этапе записи во временные папки, то временная папка с расчетами, метаинформацией и меткой будут удалены.

      val cleaner = new DirectoriesCleaner(fs)

      cleaner.cleanUp(Seq(tmpStorageDir))
      throw e
    }
  }

}
