package data.alben.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object HelloSparkSQL extends Serializable {

  def main(args: Array[String]): Unit = {

    @transient lazy val logger : Logger = Logger.getLogger(getClass.getName)

    if (args(0) == 0) {
      logger.error("Usage: HelloSparkSQL filename")
      System.exit(1)
    }

    logger.info("Starting HelloSparkSQL")
    val spark = SparkSession.builder()
      .appName("HelloSparkSQL")
      .master("local[3]")
      .getOrCreate()

    val rawDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(0))

    rawDf.createOrReplaceTempView("survey_tbl")

    val countDf = spark.sql("SELECT Country, count(1) FROM survey_tbl WHERE Age < 40 GROUP BY Country")

    logger.info(countDf.collect().mkString(","))

    logger.info("Finished HelloSparkSQl")

    scala.io.StdIn.readLine()
    spark.stop()
  }

}
