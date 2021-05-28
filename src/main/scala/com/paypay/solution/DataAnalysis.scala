package com.paypay.solution

import com.paypay.solution.Helper.{NUM_MOST_ENGAGED_USERS, getDataset, getSparkSession, save}
import com.paypay.utils.ELBAccessLog
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * This object performs different data analysis tasks mentioned in the exercise
  */
object DataAnalysis  extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val spark = getSparkSession()
    val logsDs = getDataset(spark)
    val sessionizedLogsDs = SessionizeByWebLog.getSessionizedWebLog(logsDs, spark)
    val averageSessionDuration = getAverageSessionDurationPerUser(sessionizedLogsDs, spark)
    save(averageSessionDuration, "output/avg_session_duration")

    val numUniqueUrlsPerSession = getNumUniqueUrlsPerSession(sessionizedLogsDs, spark)
    save(numUniqueUrlsPerSession, "output/num_unique_urls_per_session")

    // Getting top 10 engaged users
    val mostEngagedUsers = getMostEngagedUsers(sessionizedLogsDs, spark, NUM_MOST_ENGAGED_USERS)
    save(mostEngagedUsers, "output/most_engaged_users")
  }

  /**
    * Getting the average session duration for each user
    * @param sessionizedLogsDs
    * @param spark
    * @return output dataframe has user ip:port and userAgent and the average session duration of that user.
    */
  def getAverageSessionDurationPerUser(sessionizedLogsDs: Dataset[ELBAccessLog], spark:SparkSession): DataFrame = {
    import spark.implicits._
    val userSessionWindow = Window.partitionBy('clientAddress, 'userAgent, 'sessionNum)
    // Get the difference of the maximum and minimum of the dateTime as session duration inside each distinct session
    sessionizedLogsDs
      .withColumn(
        "rowNum_dateTimeAsc",
        row_number().over(userSessionWindow.orderBy('dateTime.asc))
      )
      .withColumn(
        "rowNum_dateTimeDesc",
        row_number().over(userSessionWindow.orderBy('dateTime.desc))
      )
      .groupBy('clientAddress, 'userAgent, 'sessionNum)
      .agg((
          max(when('rowNum_dateTimeDesc === lit(1), 'dateTime)) -
          max(when('rowNum_dateTimeAsc === lit(1), 'dateTime))
          ).alias("sessionDuration")
      )
      .groupBy('clientAddress, 'userAgent)
      .agg(
        avg('sessionDuration)
          .alias("averageSessionDurationInMillis")
      )
    }

  /**
    * Get the number of unique urls visited by each user per session
    * @param sessionizedLogsDs
    * @param spark
    * @return output dataframe has client ip:port, userAgent, sessionNum and number of unique urls visited by that user in that session.
    */
  def getNumUniqueUrlsPerSession(sessionizedLogsDs: Dataset[ELBAccessLog], spark:SparkSession): DataFrame = {
    import spark.implicits._
    // Get a distinct count of requestedUrl for each session
    sessionizedLogsDs
      .groupBy('clientAddress, 'userAgent, 'sessionNum)
      .agg(
        countDistinct('requestedUrl)
          .alias("numUniqueUrls")
      )
  }

  /**
    * Get the most engaged users
    * @param sessionizedLogsDs
    * @param spark
    * @param numUsers controls how many top engaged users we want in the output
    * @return output dataframe has user ip:port, userAgent, session duration by that user and rank sorted in ascending order of rank.
    */
  def getMostEngagedUsers(sessionizedLogsDs: Dataset[ELBAccessLog], spark:SparkSession, numUsers: Int): DataFrame = {
    import spark.implicits._
    // Calculate the session duration and apply rank over it
    // Based on numUsers, filter the generated dataframe
    val userSessionWindow = Window.partitionBy('clientAddress, 'userAgent, 'sessionNum)
    sessionizedLogsDs
      .withColumn("rowNum_dateTimeAsc", row_number().over(userSessionWindow.orderBy('dateTime.asc)))
      .withColumn("rowNum_dateTimeDesc", row_number().over(userSessionWindow.orderBy('dateTime.desc)))
      .groupBy('clientAddress, 'userAgent, 'sessionNum)
      .agg((max(when('rowNum_dateTimeDesc === lit(1), 'dateTime)) - max(when('rowNum_dateTimeAsc === lit(1), 'dateTime))).alias("sessionDurationInMillis"))
      .withColumn("rank", rank().over(Window.orderBy('sessionDurationInMillis.desc)))
      .filter('rank < lit(numUsers))
      .orderBy('rank)
      .select('clientAddress, 'userAgent, 'sessionDurationInMillis, 'rank)
  }
}
