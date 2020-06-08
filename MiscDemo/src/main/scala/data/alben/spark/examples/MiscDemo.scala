package data.alben.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}

object MiscDemo extends Serializable {

  @transient lazy val logger : Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Starting MiscDemo")

    val spark = SparkSession.builder()
      .appName("MiscDemo")
      .master("local[3]")
      .getOrCreate()

    val dataList = List(
      ("John","28","1","2002"),
      ("Taylor","13","12","89"),
      ("Hailee","22","11","96"),
      ("Eva","28","1","1"),
      ("Taylor","13","12","89")
    )

    val rawDf = spark.createDataFrame(dataList).toDF("name","day","month","year").repartition(3)

    val finalDf = rawDf.withColumn("id", monotonically_increasing_id())
      .withColumn("name", col("name").cast(StringType))
      .withColumn("day", col("day").cast(IntegerType))
      .withColumn("month", col("month").cast(IntegerType))
      .withColumn("year", col("year").cast(IntegerType))
      .withColumn("year", expr(
        """
          |case
          | when year < 21 then year + 2000
          | when year < 100 then year + 1900
          | else year
          |end
          |""".stripMargin))

    finalDf.show()

    val newFinalDf = rawDf.withColumn("id", monotonically_increasing_id())
      .withColumn("name", col("name").cast(StringType))
      .withColumn("day", col("day").cast(IntegerType))
      .withColumn("month", col("month").cast(IntegerType))
      .withColumn("year", col("year").cast(IntegerType))
      .withColumn("year",
          when(col("year") < 21, col("year") + 2000)
          when(col("year") < 100, col("year") + 1900)
          otherwise(col("year"))
        )
      .withColumn("date_of_birth", to_date(expr("concat(year,'-',month,'-',day)"), "y-M-d"))
      .drop("year","month","day")
      .dropDuplicates("name", "date_of_birth")
      .sort(expr("date_of_birth desc"))

    newFinalDf.show()

    logger.info("Finished MiscDemo")
    spark.stop()
  }

}
