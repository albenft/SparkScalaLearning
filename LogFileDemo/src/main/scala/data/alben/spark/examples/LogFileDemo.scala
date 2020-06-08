package data.alben.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object LogFileDemo extends Serializable {

  @transient lazy val logger : Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("Starting LogFileDemo")
    val spark = SparkSession.builder()
      .appName("LogFileDemo")
      .master("local[3]")
      .getOrCreate()

    // TODO

    logger.info("Finished LogFileDemo")
    spark.stop()

  }

}
