package data.alben.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object AggDemo extends Serializable {

  @transient lazy val logger : Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("Starting AggDemo")
    val spark = SparkSession.builder()
      .appName("AggDemo")
      .master("local[3]")
      .getOrCreate()

    // TODO

    logger.info("Finished AggDemo")
    spark.stop()
  }

}
