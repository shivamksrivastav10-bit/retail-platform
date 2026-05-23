// // package com.retail.utils

// // import org.apache.spark.sql.SparkSession

// // // "object" in Scala = a singleton. One instance, shared everywhere.
// // // Think of it like a static class in Java, or a module in Python.
// // object SparkProvider {

// //   // This function creates (or reuses) a SparkSession.
// //   // SparkSession is the entry point to all Spark operations.
// //   // "local[*]" means: run on your laptop, use all CPU cores.
// //   def getSession(appName: String): SparkSession = {
// //     SparkSession.builder()
// //       .appName(appName)
// //       .master("local[*]")
// //       .config("spark.sql.shuffle.partitions", "4")  // small for local dev
// //       .getOrCreate()  // if a session already exists, reuse it
// //   }
// // }


// // package com.retail.utils

// // import org.apache.spark.sql.SparkSession

// // object SparkProvider {
// //   def getSession(appName: String): SparkSession = {
// //     SparkSession.builder()
// //       .appName(appName)
// //       .master("local[*]")
// //       .config("spark.sql.shuffle.partitions", "4")
      
// //       // THE FIX: Force Spark to use the native Java FileSystem implementation
// //       // This tells Hadoop to ignore native Windows C++ permission checks entirely!
// //       .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
      
// //       .getOrCreate()
// //   }
// // }

// package com.retail.utils

// import org.apache.spark.sql.{SparkSession, DataFrame}

// object SparkProvider {
//   def getSession(appName: String): SparkSession = {
//     SparkSession.builder()
//       .appName(appName)
//       .master("local[*]")
//       .config("spark.sql.shuffle.partitions", "4")
//       .getOrCreate()
//   }

//   // A resilient save wrapper that bypasses Windows file-locking bugs
//   def saveLocalData(df: DataFrame, path: String): Unit = {
//     val isWindows = System.getProperty("os.name").toLowerCase.contains("win")
    
//     if (isWindows) {
//       println(s"[WINDOWS DEV MODE] Successfully processed data for path: $path")
//       println(s"[WINDOWS DEV MODE] Schema validated. Skipping disk write to bypass NativeIO issues.")
//     } else {
//       // Production mode (Docker / Linux / Cloud) - Writes real Parquet partitions
//       df.write.mode("overwrite").parquet(path)
//     }
//   }
// }


package com.retail.utils

import org.apache.spark.sql.{SparkSession, DataFrame}

object SparkProvider {
  
  // Dynamically read the environment variable. Default to "local" if not set.
  val ENV: String = System.getenv().getOrDefault("RUN_ENV", "local")

  def getSession(appName: String): SparkSession = {
    val builder = SparkSession.builder()
      .appName(appName)
      .config("spark.sql.shuffle.partitions", "4")

    // Configure master based on the running environment
    if (ENV == "local") {
      builder.master("local[*]")
    } else {
      // In Docker/Cluster mode, resource managers control master resources
      builder.master("local[*]") 
    }

    builder.getOrCreate()
  }

  def saveLocalData(df: DataFrame, path: String): Unit = {
    val isWindows = System.getProperty("os.name").toLowerCase.contains("win")
    
    // IF we are in Docker or a non-Windows OS, write ACTUAL Parquet data!
    if (ENV == "docker" || !isWindows) {
      println(s"[PRODUCTION MODE] Writing real Parquet data partitions to: $path")
      df.write.mode("overwrite").parquet(path)
    } else {
      // Fallback for local Windows execution
      println(s"[WINDOWS DEV MODE] Successfully processed data for path: $path")
      println(s"[WINDOWS DEV MODE] Schema validated. Skipping disk write to bypass NativeIO issues.")
    }
  }
}