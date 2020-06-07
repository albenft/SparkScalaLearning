package data.alben.spark.examples

import java.util.Properties

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source


object HelloSpark extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      logger.error("Usage: HelloSpark filename")
      System.exit(1)
    }

    logger.info("Starting Hello Spark")

    val spark = SparkSession.builder()
      .config(getSparkAppConf)
      .getOrCreate()

    // Process Data
    val rawSurveyDf = loadSurveyDf(spark, args(0))
    val partitionedSurveyDf = rawSurveyDf.repartition(2)
    val transformedSurveyDf = countByCountry(partitionedSurveyDf)

    transformedSurveyDf.foreach(row => {
      logger.info("Country: " + row.getString(0) + " Count: " + row.getLong(1))
    })

    logger.info(transformedSurveyDf.collect().mkString("->"))

    logger.info("Finished Hello Spark")

    scala.io.StdIn.readLine()
    spark.stop()
  }

  def getSparkAppConf: SparkConf = {
    val sparkAppConf = new SparkConf()

    val props = new Properties()
    props.load(Source.fromFile("spark.conf").bufferedReader())
    props.forEach((k, v) => sparkAppConf.set(k.toString, v.toString))

    sparkAppConf
  }

  def loadSurveyDf(spark: SparkSession, datafile: String) : DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(datafile)
  }

  def countByCountry(surveyDf: DataFrame) : DataFrame = {
    surveyDf
      .where("age < 40")
      .select("Age","Gender", "Country", "state")
      .groupBy("Country")
      .count()
  }
}
