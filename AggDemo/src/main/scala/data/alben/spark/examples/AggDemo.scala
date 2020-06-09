package data.alben.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggDemo extends Serializable {

  @transient lazy val logger : Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("Starting AggDemo")
    val spark = SparkSession.builder()
      .appName("AggDemo")
      .master("local[3]")
      .getOrCreate()

    // TODO
    val invoiceDf = spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/invoices.csv")

    /*
    invoiceDf.show(5)



    // Spark
    invoiceDf.select(
      count("*").as("Count Rows"),
      sum("Quantity").as("Total Qty"),
      avg("UnitPrice").as("Avg Price"),
      countDistinct("InvoiceNo").as("Count Inv"),
      countDistinct("CustomerID").as("Unique Cust")
    ).show()

    // SQL like syntax
    invoiceDf.selectExpr(
      "count(1) AS `Count Rows`",
      "sum(Quantity) AS `Total Qty`",
      "avg(UnitPrice) AS `Avg Price`"
    ).show()


     */

    // Using Temporary SQL Table
    invoiceDf.createTempView("sales")

    // Using SQL to group
    val sqlSummarizedDf = spark.sql(
      """
        |SELECT
        | Country,
        | InvoiceNo,
        | sum(Quantity) AS Total_Quantity,
        | round(sum(Quantity * UnitPrice), 2) AS Invoice_Value
        |FROM
        | sales
        |GROUP BY
        | 1,2
        |""".stripMargin)
    sqlSummarizedDf.show(10, false)

    // Using Spark
    val summarizedDf = invoiceDf.groupBy("Country", "InvoiceNo")
        .agg(
          sum("Quantity").as("Total_Quantity"),
          round(sum(expr("Quantity * UnitPrice")), 2).as("Invoice_Value")
        )
    summarizedDf.show(10)


    logger.info("Finished AggDemo")
    spark.stop()
  }

}
