package data.alben.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, Row, SparkSession}

case class SurveyRecord(Age: Int, Country: String, Gender: String, state: String)

object HelloDataset extends Serializable {

  @transient lazy val logger : Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      logger.error("Usage: HelloDataset filename")
      System.exit(1)
    }

    logger.info("Starting Hello Dataset")

    val spark = SparkSession.builder()
      .appName("HelloDataset")
      .master("local[3]")
      .getOrCreate()

    val rawDf: Dataset[Row] = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(0))

    import spark.implicits._
    val surveyDs: Dataset[SurveyRecord] = rawDf.select("Age", "Country", "Gender", "state").as[SurveyRecord]

    val filteredDs = surveyDs.filter(row => row.Age < 40)
    val filteredDf = surveyDs.filter("Age < 40")

    val countDs = filteredDs.groupByKey(r => r.Country).count()
    val countDf = filteredDf.groupBy("Country").count()

    logger.info("Dataset: " + countDs.collect().mkString(","))
    logger.info("Dataframe: " + countDf.collect().mkString(","))

    spark.stop()
  }

}
