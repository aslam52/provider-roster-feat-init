package com.availity.spark.provider

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec

class ProviderRosterSpec extends AnyFunSpec with DataFrameComparer with BeforeAndAfterEach {

  implicit var spark: SparkSession = _

  override def beforeEach(): Unit = {
    // Set hadoop.home.dir property
    System.setProperty("hadoop.home.dir", "E:\\hadoop-3.4.0\\hadoop-3.4.0")

    var spark = SparkSession.builder()
      .appName("Provider Visits")
      .master("local[*]")
      .config("spark.ui.enabled", "false") // Disable the Spark UI
      .config("spark.sql.shuffle.partitions", "8") // Configure shuffle partitions for local mode
      .config("spark.driver.bindAddress", "127.0.0.1") // Ensure the driver binds to the local address
      .config("spark.driver.host", "127.0.0.1") // Explicitly set the driver host
      .getOrCreate()
  }

  override def afterEach(): Unit = {
    spark.stop()
  }

  describe("process") {
    it("should calculate the total number of visits per provider") {
      // Define test data
      val providersCSV = Seq(
        "provider_id,provider_specialty,first_name,middle_name,last_name",
        "47259,Urology,Bessie,B,Kuphal",
        "17847,Cardiology,Candice,C,Block"
      ).mkString("\n")

      val visitsCSV = Seq(
        "40838,47259,2022-08-23",
        "58362,47259,2022-03-25",
        "65989,47259,2021-10-28",
        "69139,47259,2022-02-04"
      ).mkString("\n")

      val providersPath = "data/providers.csv"
      val visitsPath = "data/visits.csv"
      val outputPath = "output/test"

      // Write test data to files
      import java.nio.file.{Files, Paths, StandardOpenOption}
      Files.write(Paths.get(providersPath), providersCSV.getBytes, StandardOpenOption.CREATE)
      Files.write(Paths.get(visitsPath), visitsCSV.getBytes, StandardOpenOption.CREATE)

      // Run the process
      ProviderRoster.process(providersPath, visitsPath, outputPath)

      // Read the output data
      val resultDF = spark.read.json(s"$outputPath/total_visits_per_provider/provider_specialty=Urology")

      resultDF.show()
      // Expected DataFrame

      import spark.implicits._
      val expectedDF = Seq(
        (47259, "Bessie", "B", "Kuphal", "Urology", 4)
      ).toDF("provider_id", "first_name", "middle_name", "last_name", "provider_specialty", "number_of_visits")

      // Compare DataFrames
      assertSmallDataFrameEquality(resultDF, expectedDF, ignoreNullable = true)
    }
  }
}
