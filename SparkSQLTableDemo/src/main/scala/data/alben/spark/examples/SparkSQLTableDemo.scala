package data.alben.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSQLTableDemo extends Serializable {

  @transient lazy val logger : Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("Starting SparkSQLTableDemo")
    val spark = SparkSession.builder()
      .appName("SparkSQLTableDemo")
      .master("local[3]")
      .enableHiveSupport()
      .getOrCreate()

    val flightTimeDf = spark.read
      .format("parquet")
      .option("header", "true")
      .option("path", "dataSource/flight*.parquet")
      .load()

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    flightTimeDf.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("flight_time_tbl")

    spark.catalog.listTables("AIRLINE_DB")

    logger.info("Finished SparkSQLTableDemo")
    spark.stop()
  }
}
