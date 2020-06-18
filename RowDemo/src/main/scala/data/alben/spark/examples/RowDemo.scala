package data.alben.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object RowDemo extends Serializable {

  @transient lazy val logger : Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

//    if(args.length == 0) {
//      logger.error("Usage: RowDemo filename.")
//      System.exit(1)
//    }

    logger.info("Starting RowDemo")
    val spark = SparkSession.builder()
      .appName("RowDemo")
      .master("local[3]")
      .getOrCreate()

    // TODO
    val mySchema = StructType(List(
      StructField("ID", StringType),
      StructField("EvnDate", StringType)
    ))

    val myRows = List(Row("123","04/05/2020"), Row("124","04/06/2020"), Row("125","04/07/2020"))
    val myRDD = spark.sparkContext.parallelize(myRows, 2)
    val myDf = spark.createDataFrame(myRDD, mySchema)

    myDf.printSchema
    myDf.show

    val newDf = toDateDf(myDf, "M/d/y", "EvnDate")

    newDf.printSchema
    newDf.show

    logger.info("Finished RowDemo")
    spark.stop()
  }

  def toDateDf(df: DataFrame, date_format: String, date_column: String): DataFrame = {
    df.withColumn(date_column, to_date(col(date_column), date_format))
  }

}
