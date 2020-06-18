package data.alben.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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

    /* My code
    val transformedDf = invoiceDf.withColumn("InvoiceDate", to_date(col("InvoiceDate"), "dd-MM-yyyy H.m"))
      .withColumn("WeekNumber", weekofyear(col("InvoiceDate")))

    val aggDf = transformedDf.groupBy("Country","WeekNumber")
        .agg(
          countDistinct("InvoiceNo").as("NumInvoices"),
          sum("Quantity").as("TotalQty"),
          round(sum(expr("Quantity * UnitPrice")), 2).as("InvoiceValue")
        ).orderBy(col("Country"), col("WeekNumber"))

    aggDf.show(10, false)
     */

    // Course code

    val numInvoices = countDistinct("InvoiceNo").as("NumInvoices")
    val totalQty = sum("Quantity").as("TotalQty")
    val invoiceValue = round(sum(expr("Quantity * UnitPrice")), 2).as("InvoiceValue")

    val summaryDf = invoiceDf.withColumn("InvoiceDate", to_date(col("InvoiceDate"), "dd-MM-yyyy H.m"))
      .where("year(InvoiceDate) == 2010")
      .withColumn("WeekNumber", weekofyear(col("InvoiceDate")))
      .groupBy(col("Country"), col("WeekNumber"))
      .agg(numInvoices, totalQty, invoiceValue)

    summaryDf.write
      .format("parquet")
      .mode("overwrite")
      .save("dataSink/output")

    summaryDf.sort("Country", "WeekNumber").show(10)

    logger.info("Finished GroupingDemo")
    spark.stop()

  }

}
