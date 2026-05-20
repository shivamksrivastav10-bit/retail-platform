package com.retail.silver

import com.retail.utils.SparkProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object SilverJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkProvider.getSession("Silver Transformation")
    import spark.implicits._

    println("[SILVER] Starting Silver Layer transformation pipeline...")

    // 1. In a normal environment, we would read our Bronze Parquet directories.
    // Because we are on a Windows host, we will read the raw source files directly 
    // to simulate the pipeline securely.

    
// First of all, look at that console output! [success] Total time: 29 s and an inner-joined dataset showing customer names, cities, and order amounts perfectly aligned. Your Silver layer logic ran flawlessly.

// To answer your question: in a production environment (like on a Linux server, inside Docker, or in the Cloud), you wouldn't read the raw CSV inputs again. Instead, your Silver layer would pick up exactly where the Bronze layer left off by reading the Parquet files that Bronze wrote.

// Here is the exact code you would use to do that:

// The Code to Read Parquet Directories
// In Spark, reading Parquet is incredibly simple because Parquet is Spark's default storage format. Instead of .csv(), you just use .parquet():

// Scala
// // How you would read the Bronze Parquet data in production:
// val ordersRaw = spark.read.parquet("data/output/bronze/orders")
// val customersRaw = spark.read.parquet("data/output/bronze/customers")
// Why is this better than reading CSVs? (The Concept)
// When you use spark.read.parquet(), two amazing things happen under the hood that make production data engineering fast:

// No Schema Inference Needed: CSVs are just plain text, so Spark has to read the file to guess what is a number and what is a string (which is slow). Parquet files contain a built-in schema blueprint. Spark reads the metadata instantly and knows exactly what the column types are.

// Columnar Compression: If your Silver job only needs customer_id and amount to do a calculation, Spark will completely skip reading the other columns on disk.

// Ready for the Gold Layer?
// Now that your Silver layer has successfully cleaned and merged the data into an enriched view, we are ready for Phase 2.4: The Gold Layer.

// In the Medallion Architecture, the Gold layer is the business-level aggregation layer. This is where you calculate high-level KPIs that business analysts, PowerBI dashboards, or executive teams care about (e.g., What is the total spend per customer segment?).



    val ordersRaw = spark.read.option("header", "true").option("inferSchema", "true").csv("data/input/orders/orders.csv")
    val customersRaw = spark.read.option("header", "true").option("inferSchema", "true").csv("data/input/customers/customers.csv")

    // 2. Data Cleaning: Filter out invalid orders (e.g., amount must be greater than 0 and not null)
    val cleanedOrders = ordersRaw
      .filter($"amount".isNotNull && $"amount" > 0)
      .withColumn("amount", $"amount".cast(DoubleType))

    // 3. Data Cleaning: Handle text normalization for Customers (Trim spaces, standard casing)
    val cleanedCustomers = customersRaw
      .filter($"customer_id".isNotNull)
      .withColumn("city", trim(initcap($"city"))) // Standardizes "bangalore" or "BANGALORE" to "Bangalore"

    // 4. Data Enrichment: Inner join Orders and Customers on customer_id
    val enrichedTransactions = cleanedOrders.join(
      cleanedCustomers,
      Seq("customer_id"), 
      "inner"
    )

    // Preview our pristine Silver dataset
    println(s"[SILVER] Enriched Transactions Count: ${enrichedTransactions.count()} rows")
    enrichedTransactions.show()

    // 5. Save the enriched data safely using our Windows bypass mechanism
    SparkProvider.saveLocalData(enrichedTransactions, "data/output/silver/enriched_transactions")

    println("[SILVER] Done.")
    spark.stop()
  }
}