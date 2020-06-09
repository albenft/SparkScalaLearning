package data.alben.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object GroupingDemo extends Serializable {

  @transient lazy val logger : Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("Starting GroupingDemo")
    val spark = SparkSession.builder()
      .appName("GroupingDemo")
      .master("local[3]")
      .getOrCreate()

    val invoiceDf = spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/invoices.csv")

    invoiceDf.show(5)

    logger.info("Finished GroupingDemo")
    spark.stop()

  }

}
