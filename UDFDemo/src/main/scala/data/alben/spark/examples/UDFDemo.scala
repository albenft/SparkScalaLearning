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

    val surveyDF = spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(args(0))


    import org.apache.spark.sql.functions._
    val parseGenderUDF = udf(parseGender(_: String): String)

    spark.catalog.listFunctions().filter(r => r.name == "parseGenderUDF").show()

    val transformedSurveyDF = surveyDF.withColumn("Gender", parseGenderUDF(col("Gender")))
    transformedSurveyDF.show(10, false)

    spark.udf.register("parseGenderUDF", parseGender(_: String): String)
    spark.catalog.listFunctions().filter(r => r.name == "parseGenderUDF").show()

    val transformedSurveyDF2 = surveyDF.withColumn("Gender", expr("parseGenderUDF(Gender)"))
    transformedSurveyDF2.show(10, false)

    logger.info("Finished UDFDemo")
    spark.stop()
  }

  def parseGender(gender: String): String = {
    val femalePattern = "^f$|f.m|w.m".r
    val malePattern = "^m$|ma|m.l".r

    if(femalePattern.findFirstIn(gender.toLowerCase).nonEmpty) "Female"
    else if(malePattern.findFirstIn(gender.toLowerCase).nonEmpty) "Male"
    else "Unknown"
  }

}
