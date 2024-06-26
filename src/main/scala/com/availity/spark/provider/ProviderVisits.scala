package com.availity.spark.provider

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ProviderVisits {

  def main(args: Array[String]): Unit = {
    println("----Main----")

    // Set hadoop.home.dir property
    System.setProperty("hadoop.home.dir", "E:\\hadoop-3.4.0\\hadoop-3.4.0")

    val spark = SparkSession.builder()
      .appName("Provider Visits")
      .master("local[*]")
      .config("spark.ui.enabled", "false")  // Disable the Spark UI
      .config("spark.sql.shuffle.partitions", "8") // Configure shuffle partitions for local mode
      .config("spark.driver.bindAddress", "127.0.0.1") // Ensure the driver binds to the local address
      .config("spark.driver.host", "127.0.0.1")  // Explicitly set the driver host
      .getOrCreate()

    // Load providers data with headers
    val providersPath = "data/providers.csv"
    val providersDF = spark.read
      .option("header", "true")
      .option("sep", "|")  // Specify the pipe delimiter
      .csv(providersPath)

    // Define schema for visits data
    val visitsSchema = StructType(Array(
      StructField("visit_id", LongType, nullable = true),
      StructField("provider_id", LongType, nullable = true),
      StructField("visit_date", StringType, nullable = true)
    ))

    // Load visits data without headers
    val visitsPath = "data/visits.csv"
    val visitsDF = spark.read
      .schema(visitsSchema)
      .csv(visitsPath)

    // Cast necessary columns to appropriate types
    val visitsDFTyped = visitsDF
      .withColumn("visit_date", to_date(col("visit_date")))

    // Task 1: Total number of visits per provider
    val visitsPerProvider = visitsDFTyped.groupBy("provider_id")
      .count()
      .withColumnRenamed("count", "number_of_visits")

    val report1 = visitsPerProvider.join(providersDF, "provider_id")
      .select("provider_id", "first_name", "middle_name", "last_name", "provider_specialty", "number_of_visits")

    report1.show()
    report1.write.mode("overwrite").partitionBy("provider_specialty").json("output/total_visits_per_provider")

    // Task 2: Total number of visits per provider per month
    val visitsPerMonth = visitsDFTyped
      .groupBy(col("provider_id"), date_format(col("visit_date"), "yyyy-MM").alias("month"))
      .count()
      .withColumnRenamed("count", "number_of_visits")

    visitsPerMonth.show()
    visitsPerMonth.write.mode("overwrite").json("output1/total_visits_per_provider_per_month")

    spark.stop()
  }
}
