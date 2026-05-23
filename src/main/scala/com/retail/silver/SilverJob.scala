package com.retail.silver

import com.retail.utils.SparkProvider
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object SilverJob {

  // The Testable Core: Logic remains identical
  def transform(ordersDF: DataFrame, customersDF: DataFrame): DataFrame = {
    val cleanOrders = ordersDF
      .filter(col("amount") > 0 && col("order_id").isNotNull)
      .withColumn("amount", col("amount").cast("double"))

    cleanOrders.join(customersDF, "customer_id")
      .select(
        col("customer_id"),
        col("order_id"),
        col("amount"),
        col("order_date"),
        col("name"),
        col("city"),
        col("segment")
      )
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkProvider.getSession("Silver Transformation")

    println("[SILVER] Starting Silver Layer transformation pipeline...")

    // 1. Read from Bronze Delta tables in MinIO
    val ordersRaw = spark.read.format("delta").load("s3a://retail/bronze/orders")
    val customersRaw = spark.read.format("delta").load("s3a://retail/bronze/customers")

    // 2. Call the core transformation
    val enrichedTransactions = transform(ordersRaw, customersRaw)

    println(s"[SILVER] Enriched Transactions Count: ${enrichedTransactions.count()} rows")
    enrichedTransactions.show()

    // 3. Write to Silver Delta table in MinIO using the resilient wrapper
    SparkProvider.saveLocalData(enrichedTransactions, "s3a://retail/silver/enriched_transactions")

    println("[SILVER] Done.")
    spark.stop()
  }
}