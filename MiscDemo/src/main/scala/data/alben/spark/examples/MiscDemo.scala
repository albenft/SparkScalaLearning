package data.alben.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object MiscDemo extends Serializable {

  @transient lazy val logger : Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Starting MiscDemo")

    val spark = SparkSession.builder()
      .appName("MiscDemo")
      .master("local[3]")
      .getOrCreate()

    logger.info("Finished MiscDemo")
    spark.stop()
  }

}
