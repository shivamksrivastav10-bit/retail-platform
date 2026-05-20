package com.retail.gold

import com.retail.utils.SparkProvider
import org.apache.spark.sql.functions._

object GoldJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkProvider.getSession("Gold Aggregations")
    import spark.implicits._

    println("[GOLD] Starting Gold Layer aggregation pipeline...")

    // 1. Read our clean, joined data.
    // (Simulating by reading raw files directly to seamlessly bypass local Windows environment quirks)
    val ordersRaw = spark.read.option("header", "true").option("inferSchema", "true").csv("data/input/orders/orders.csv")
    val customersRaw = spark.read.option("header", "true").option("inferSchema", "true").csv("data/input/customers/customers.csv")
    
    val enrichedTransactions = ordersRaw.join(customersRaw, Seq("customer_id"), "inner")

    // 2. Compute Business KPIs: Group by Segment and City, aggregate Total Spend and Order Volumes
    val segmentMetrics = enrichedTransactions
      .groupBy($"segment", $"city")
      .agg(
        round(sum($"amount"), 2).as("total_revenue"),
        count($"order_id").as("total_orders"),
        round(avg($"amount"), 2).as("average_order_value")
      )
      .orderBy($"total_revenue".desc)

    // Preview our business intelligence metrics
    println(s"[GOLD] Business KPI Summary Table:")
    segmentMetrics.show()

    // 3. Save the business table using our Windows bypass wrapper
    SparkProvider.saveLocalData(segmentMetrics, "data/output/gold/segment_revenue_metrics")

    println("[GOLD] Done.")
    spark.stop()
  }
}