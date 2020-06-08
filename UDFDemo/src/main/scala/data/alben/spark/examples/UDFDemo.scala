package data.alben.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object UDFDemo extends Serializable {
  
  @transient lazy val logger : Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    if(args.length == 0) {
      logger.error("Usage: UDFDemo Filename.")
      System.exit(1)
    }

    logger.info("Starting UDF Demo")
    val spark = SparkSession.builder()
      .appName("UDFDemo")
      .master("local[3]")
      .getOrCreate()

    // TODO

    logger.info("Finished UDFDemo")
    spark.stop()
  }

}
