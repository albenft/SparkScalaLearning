package data.alben.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

case class SurveyRecord(Age: Int, Gender: String, Country: String, state: String)

object HelloRDD {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      logger.error("Usage: HelloRDD filename")
      System.exit(1)
    }

    logger.info("Starting Hello RDD")

    val sparkAppConf = new SparkConf().setAppName("HelloRDD").setMaster("local[3]")
    val sparkContext = new SparkContext(sparkAppConf)

    logger.info("Create RDD from file")
    val linesRDD = sparkContext.textFile(args(0))

    logger.info("Repartition the RDD")
    val partitionedRDD = linesRDD.repartition(2)

    logger.info("Transform RDD to array of string")
    val colsRDD = partitionedRDD.map(line => line.split(",").map(_.trim))

    logger.info("Select desired columns from RDD")
    val selectRDD = colsRDD.map(col => SurveyRecord(col(1).toInt, col(2), col(3), col(4)))

    logger.info("Filter row based on Age")
    val filteredRDD = selectRDD.filter(row => row.Age < 40)

    logger.info("Create (key,value) pair RDD to be used as grouping)")
    val kvRDD = filteredRDD.map(row => (row.Country, 1))

    logger.info("Get count result")
    val countRDD = kvRDD.reduceByKey((v1,v2) => v1 + v2)

    logger.info("Finished Hello RDD")

    logger.info("Keeping spark running")
//    scala.io.StdIn.readLine()
    sparkContext.stop()

  }

}
