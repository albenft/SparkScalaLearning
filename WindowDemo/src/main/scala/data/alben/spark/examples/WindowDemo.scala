package data.alben.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowDemo extends Serializable {

  @transient lazy val logger : Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("Starting WindowDemo")

    val spark = SparkSession.builder()
      .appName("WindowDemo")
      .master("local[3]")
      .getOrCreate()

    val summaryDf = spark.read.parquet("data/summary.parquet")
//    invoiceDf = spark.read
//        .format("csv")
//        .option("header", "true")
//        .option("inferSchema", "true")
//        .load("data/invoices.csv")

    val runningWindow = Window.partitionBy("Country")
        .orderBy("WeekNumber")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    summaryDf.withColumn("RunningTotal",
      sum(col("InvoiceValue")).over(runningWindow)
    ).show()

    logger.info("Finished WindowDemo")
    spark.stop()

  }

}
