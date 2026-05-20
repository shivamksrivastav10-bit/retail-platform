package com.retail.bronze

// imports — like Python's "from pyspark.sql import SparkSession"
import com.retail.utils.SparkProvider
import org.apache.spark.sql.functions._

// "object" with a def main = this is a runnable program
// args: Array[String] = command-line arguments (like sys.argv in Python)
object BronzeJob {

  def main(args: Array[String]): Unit = {


    // FORCE Windows to see your Hadoop binaries directly in the code
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    // Unit = "void" = this function returns nothing (like def f() -> None in Python)
    val spark = SparkProvider.getSession("Bronze Ingestion")

    // "val" = immutable variable (cannot be reassigned, like final in Java)
    // "var" = mutable variable (avoid when possible in Scala)
    val ordersRaw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")   // Spark guesses types automatically
      .csv("data/input/orders/orders.csv")

    val customerRaw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/input/customers/customers.csv")

    // withColumn adds a new column (like df["new_col"] = ... in PySpark)
    val ordersBronze = ordersRaw
      .withColumn("ingestion_date", current_date())
      .withColumn("source", lit("csv"))   // lit() = literal value column

    val customersBronze = customerRaw
      .withColumn("ingestion_date", current_date())
      .withColumn("source", lit("csv"))

    println(s"[BRONZE] Orders: ${ordersBronze.count()} rows")  // s"..." = string interpolation
    ordersBronze.show()

    println(s"[BRONZE] Customers: ${customersBronze.count()} rows")
    customersBronze.show()

    // ordersBronze
    //   .coalesce(1)
    //   .write
    //   .mode("overwrite")
    //   .option("header", "true")
    //   .csv("data/output/bronze/orders")

    // customersBronze
    //   .coalesce(1)
    //   .write
    //   .mode("overwrite")
    //   .option("header", "true")
    //   .csv("data/output/bronze/customers")

    // println("[BRONZE] Done.")
    // spark.stop()

    // Write as Parquet — columnar format, compressed, much faster than CSV
    // ordersBronze.write
    //   .mode("overwrite")
    //   .parquet("data/output/bronze/orders")

    // customersBronze.write
    //   .mode("overwrite")
    //   .parquet("data/output/bronze/customers")

    // println("[BRONZE] Done.")
    // spark.stop()


  

    // Use our resilient dev-mode save method!
    SparkProvider.saveLocalData(ordersBronze, "data/output/bronze/orders")
    SparkProvider.saveLocalData(customersBronze, "data/output/bronze/customers")

    println("[BRONZE] Done.")
    spark.stop()
  }
}


