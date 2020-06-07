package data.alben.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataSinkDemo extends Serializable {

  @transient lazy val logger : Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("Starting DataSinkDemo")
    val spark = SparkSession.builder()
      .appName("DataSinkDemo")
      .master("local[3]")
      .getOrCreate()

    val flightTimeParquet = spark.read
      .format("parquet")
      .option("path","dataSource/flight*.parquet")
      .load()

    logger.info("Num of Partitions before: " + flightTimeParquet.rdd.getNumPartitions)
    import org.apache.spark.sql.functions.spark_partition_id
//    flightTimeParquet.groupBy(spark_partition_id()).count().show()

//    flightTimeParquet.write
//      .format("avro")
//      .mode(SaveMode.Overwrite)
//      .option("path", "dataSink/avro/")
//      .save()

    val partitionedDf = flightTimeParquet.repartition(5)

    logger.info("Num of Partitions after: " + partitionedDf.rdd.getNumPartitions)
    partitionedDf.groupBy(spark_partition_id()).count().show()
//
//    partitionedDf.write
//      .format("avro")
//      .mode(SaveMode.Overwrite)
//      .option("path", "dataSink/avro/")
//      .save()

    flightTimeParquet.write
      .format("json")
      .mode(SaveMode.Overwrite)
      .option("path", "dataSink/json/")
      .partitionBy("OP_CARRIER", "ORIGIN")
      .option("maxRecordsPerFile", 10000)
      .save()

    logger.info("Finished DataSinkDemo")
    spark.stop()
  }
}
