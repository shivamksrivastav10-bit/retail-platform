package com.retail.bronze

import com.retail.utils.SparkProvider
import org.apache.spark.sql.functions._

object BronzeJob {

  def main(args: Array[String]): Unit = {
    // We removed the hardcoded C:\\hadoop here to keep it container-neutral.
    // Ensure you set this as an environment variable on your local Windows machine if needed.
    
    val spark = SparkProvider.getSession("Bronze Ingestion")

    // Read the CSV data
    val ordersRaw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/input/orders/*.csv") // Changed to *.csv to handle multiple files

    val customerRaw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/input/customers/*.csv")

    // Add metadata columns
    val ordersBronze = ordersRaw
      .withColumn("ingestion_date", current_date())
      .withColumn("source", lit("csv"))

    val customersBronze = customerRaw
      .withColumn("ingestion_date", current_date())
      .withColumn("source", lit("csv"))

    println(s"[BRONZE] Orders: ${ordersBronze.count()} rows")
    ordersBronze.show()

    println(s"[BRONZE] Customers: ${customersBronze.count()} rows")
    customersBronze.show()

    // Correctly calling the SparkProvider using the processed variables
    SparkProvider.saveLocalData(ordersBronze, "s3a://retail/bronze/orders")
    SparkProvider.saveLocalData(customersBronze, "s3a://retail/bronze/customers")

    println("[BRONZE] Done.")
    spark.stop()
  }
}