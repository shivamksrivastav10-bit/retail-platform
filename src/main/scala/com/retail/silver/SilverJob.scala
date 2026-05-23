package com.retail.silver

import com.retail.utils.SparkProvider
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object SilverJob {

  // The Testable Core: Pure transformation logic, no reading or writing files!
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

    val ordersRaw = spark.read.parquet("data/output/bronze/orders")
    val customersRaw = spark.read.parquet("data/output/bronze/customers")

    // Call the core transformation
    val enrichedTransactions = transform(ordersRaw, customersRaw)

    println(s"[SILVER] Enriched Transactions Count: ${enrichedTransactions.count()} rows")
    enrichedTransactions.show()

    enrichedTransactions.write
      .mode("overwrite")
      .parquet("data/output/silver/enriched_transactions")

    println("[SILVER] Done.")
    spark.stop()
  }
}