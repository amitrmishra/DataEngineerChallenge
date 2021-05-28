package com.paypay.solution

import com.paypay.utils.ELBAccessLog
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.io.File

/**
  * Helper class to maintain constants and providing common functionalities across methods of different classes
  */
object Helper {
  /**
    * CONSTANTS
    */
  val LOG_FILE_PATH = new File("./data/2015_07_22_mktplace_shop_web_log_sample.log.gz").getAbsolutePath
  val MAX_SESSION_DURATION = 15 * 60 * 1000
  val NUM_MOST_ENGAGED_USERS = 10

  /**
    * Getting a spark session for main class as well as test classes
    * Can receive custom configurations through SparkConf object
    * @return SparkSession
    */
  def getSparkSession(): SparkSession = {
    val sparkConf = new SparkConf()

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .config(sparkConf)
      .getOrCreate()
    spark
  }

  /**
    * From a file containing web log strings, returns a Dataset of ELBAccessLog objects after filtering out malformed entries
    * @param spark
    * @return Dataset[ELBAccessLog]
    */
  def getDataset(spark: SparkSession): Dataset[ELBAccessLog] = {
    import spark.implicits._
    val logsRdd: RDD[ELBAccessLog] = spark.sparkContext
      .textFile(LOG_FILE_PATH)
      .flatMap(ELBAccessLog.get)
    logsRdd.toDS()
  }

  /**
    * Saves a dataframe to a given location in default format (ie. parquet)
    * @param df
    * @param location
    */
  def save(df: DataFrame, location: String) = {
    df.write.save(location)
  }
}
