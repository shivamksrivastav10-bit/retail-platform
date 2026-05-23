package com.retail.gold

import com.retail.utils.SparkProvider
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

object GoldJob {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkProvider.getSession("Gold Aggregation")

    logger.info("Starting Gold Layer aggregation pipeline...")

    // 1. Read from Silver Delta table in MinIO
    val enrichedTransactions = try {
      spark.read.format("delta").load("s3a://retail/silver/enriched_transactions")
    } catch {
      case e: Exception =>
        logger.error("Failed to read Silver data. Ensure Silver job completed successfully.", e)
        throw e
    }

    // 2. Compute business KPIs
    val goldKPIs = enrichedTransactions
      .groupBy("segment", "city")
      .agg(
        round(sum("amount"), 2).as("total_revenue"),
        count("order_id").as("total_orders"),
        round(avg("amount"), 2).as("average_order_value")
      )

    logger.info("Business KPI Summary calculation complete.")
    
    // Preview output
    val previewLines = goldKPIs.take(10).map(_.toString()).mkString("\n")
    logger.info(s"KPI Output Preview (Raw Rows):\n$previewLines")

    // 3. Write to Gold Delta table in MinIO using the resilient wrapper
    SparkProvider.saveLocalData(goldKPIs, "s3a://retail/gold/segment_revenue_metrics")

    logger.info("Gold Job pipeline process finalized cleanly.")
    spark.stop()
  }
}