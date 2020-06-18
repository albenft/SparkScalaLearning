package data.alben.spark.examples

import java.util.Date

import data.alben.spark.examples.RowDemo.toDateDf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

case class EventRecord(ID: String, EvnDate: String)

class RowDemoTest extends FunSuite with BeforeAndAfterAll {

  @transient var spark : SparkSession = _
  @transient var myDf : DataFrame = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("RowDemoTest")
      .master("local[3]")
      .getOrCreate()

    val mySchema = StructType(List(
      StructField("ID", StringType),
      StructField("EvnDate", StringType)
    ))

    val myRows = List(Row("123","04/05/2020"), Row("124","4/05/2020"), Row("125","04/5/2020"), Row("125","4/5/2020"))
    val myRDD = spark.sparkContext.parallelize(myRows, 2)
    myDf = spark.createDataFrame(myRDD, mySchema)

  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("Test Data Type") {
    val rowList = toDateDf(myDf, "M/d/y", "EvnDate").collectAsList()
    rowList.forEach(row =>
      assert(row.get(1).isInstanceOf[Date], "EvnDate column should be in Date format")
    )
  }

  test("Test Data Value") {
    val spark2 = spark
    import spark2.implicits._
    val rowList = toDateDf(myDf, "M/d/y", "EvnDate").as[EventRecord].collectAsList()
    rowList.forEach(row =>
      assert(row.EvnDate.toString == "2020-04-05", "Result should be 2020-04-05")
    )
  }


}
