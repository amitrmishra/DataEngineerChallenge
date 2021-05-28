package com.paypay.solution

import com.paypay.solution.Helper._
import com.paypay.utils.ELBAccessLog
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * This object adds the session information (sessionNum) in the original dataset.
  */
object SessionizeByWebLog extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val spark = getSparkSession
    val logsDs = getDataset(spark)
    val sessionizedWebLogDs = getSessionizedWebLog(logsDs, spark)
    save(sessionizedWebLogDs.toDF, "output/sessionized_web_logs")
  }

  def getSessionizedWebLog(logDataset: Dataset[ELBAccessLog], spark:SparkSession): Dataset[ELBAccessLog] = {
    import spark.implicits._
    // partition by the combination of user ip:port and userAgent
    // sort each partition by the dateTime in ascending order
    // inside each partition,
    //   if new log event occurred after a duration of MAX_SESSION_DURATION from the previous event,
    //   then mark the beginning of a new session
    // summing over the running count of the new session marker generates the sessionNum inside each partition.
    val userSessionWindow = Window.partitionBy('clientAddress, 'userAgent).orderBy('dateTime)
    logDataset
      .select(
        'clientAddress,
        'userAgent,
        'dateTime,
        'backendAddress,
        'requestedUrl,
        lag('dateTime, 1)
          .over(userSessionWindow)
          .as('prevDateTime))
      .withColumn(
        "isNewSession",
        when('dateTime.minus('prevDateTime) < lit(MAX_SESSION_DURATION), lit(0)).otherwise(lit(1)))
      .withColumn(
        "sessionNum",
        sum('isNewSession.cast("long")).over(userSessionWindow))
      .select('dateTime,
        'clientAddress,
        'userAgent,
        'backendAddress,
        'requestedUrl,
        'sessionNum
      )
      .as[ELBAccessLog]
  }
}
