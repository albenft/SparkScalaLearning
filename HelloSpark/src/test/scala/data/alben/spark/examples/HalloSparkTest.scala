package data.alben.spark.examples

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import data.alben.spark.examples.HelloSpark.{countByCountry, loadSurveyDf}

import scala.collection.mutable

class HalloSparkTest extends FunSuite with BeforeAndAfterAll {

  @transient var spark : SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
        .appName("HelloSparkTest")
        .master("local[3]")
        .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("Data File Loading") {
    val sampleDf = loadSurveyDf(spark, "data/sample.csv")
    val rCount = sampleDf.count()
    assert(rCount == 9, "record count should be 9")
  }

  test("Count By Country") {
    val sampleDf = loadSurveyDf(spark, "data/sample.csv")
    val countDf = countByCountry(sampleDf)
    val countryMap = new mutable.HashMap[String, Long]
    countDf.collect().foreach(r => countryMap.put(r.getString(0), r.getLong(1)))

    assert(countryMap("United States") == 4, ":- count for United States should be 4")
    assert(countryMap("Canada") == 2, ":- count for Canada should be 2")
    assert(countryMap("United Kingdom") == 1, ":- count for United Kingdom should be 1")

  }

}
