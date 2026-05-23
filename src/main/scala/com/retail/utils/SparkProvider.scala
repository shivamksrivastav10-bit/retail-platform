package com.retail.utils

import org.apache.spark.sql.{SparkSession, DataFrame}

object SparkProvider {
  val ENV: String = System.getenv().getOrDefault("RUN_ENV", "local")

  def getSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      
      // MinIO Configs
      .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
      .config("spark.hadoop.fs.s3a.access.key", "admin")
      .config("spark.hadoop.fs.s3a.secret.key", "password")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

      // CORRECTED Delta Lake Configs for 3.x
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
  }

  def saveLocalData(df: DataFrame, path: String): Unit = {
    val isWindows = System.getProperty("os.name").toLowerCase.contains("win")
    
    if (ENV == "docker" || !isWindows) {
      println(s"[PRODUCTION MODE] Writing to: $path")
      df.write.format("delta").mode("overwrite").save(path)
    } else {
      println(s"[WINDOWS DEV MODE] Skipping disk write for: $path")
    }
  }
}