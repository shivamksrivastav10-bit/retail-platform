package com.retail

import com.retail.silver.SilverJob
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class SilverJobTest extends AnyFunSuite with BeforeAndAfterAll {

  // Define a shared Spark Session for the test lifecycle
  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("SilverJob-UnitTest")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  // test("SilverJob.transform should filter out bad orders and enrich data with customer details") {
  //   import spark.implicits._

  //   // 1. Create Mock Input Data DataFrames
  //   val mockOrders = Seq(
  //     (1001, "C001", 1500.0, "2024-01-15"),
  //     (1002, "C002", -50.0,  "2024-01-16"), // Invalid negative amount! Should be dropped.
  //     (1003, "C001", 2200.0, "2024-01-17")
  //   ).toDF("order_id", "customer_id", "amount", "order_date")

  //   val mockCustomers = Seq(
  //     ("C001", "Ravi Kumar", "Bangalore", "Premium"),
  //     ("C002", "Priya Shah", "Mumbai", "Standard")
  //   ).toDF("customer_id", "name", "city", "segment")

  //   // 2. Execute the transform function
  //   val resultDF = SilverJob.transform(mockOrders, mockCustomers)

  //   // 3. Assert and Verify the results
  //   val resultArray = resultDF.collect()
    
  //   // Check that the negative amount order was removed (3 orders input -> 2 rows output)
  //   assert(resultArray.length == 2, "Expected exactly 2 enriched rows, bad rows should be filtered.")

  //   // Check that row values were successfully joined and mapped
  //   val firstRow = resultArray.find(_.getAs[Int]("order_id") == 1001).get
  //   assert(firstRow.getAs[String]("name") == "Ravi Kumar")
  //   assert(firstRow.getAs[String]("city") == "Bangalore")
  // }
  test("SilverJob.transform should filter out bad orders and enrich data with customer details") {
    // Capture the mutable var into a local stable val so Scala can safely import implicits
    val sparkSession = spark
    import sparkSession.implicits._

    // 1. Create Mock Input Data DataFrames
    val mockOrders = Seq(
      (1001, "C001", 1500.0, "2024-01-15"),
      (1002, "C002", -50.0,  "2024-01-16"), // Invalid negative amount! Should be dropped.
      (1003, "C001", 2200.0, "2024-01-17")
    ).toDF("order_id", "customer_id", "amount", "order_date")

    val mockCustomers = Seq(
      ("C001", "Ravi Kumar", "Bangalore", "Premium"),
      ("C002", "Priya Shah", "Mumbai", "Standard")
    ).toDF("customer_id", "name", "city", "segment")

    // 2. Execute the transform function
    val resultDF = SilverJob.transform(mockOrders, mockCustomers)

    // 3. Assert and Verify the results
    val resultArray = resultDF.collect()
    
    // Check that the negative amount order was removed (3 orders input -> 2 rows output)
    assert(resultArray.length == 2, "Expected exactly 2 enriched rows, bad rows should be filtered.")

    // Check that row values were successfully joined and mapped
    val firstRow = resultArray.find(_.getAs[Int]("order_id") == 1001).get
    assert(firstRow.getAs[String]("name") == "Ravi Kumar")
    assert(firstRow.getAs[String]("city") == "Bangalore")
  }
}