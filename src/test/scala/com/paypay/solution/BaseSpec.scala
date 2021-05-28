package com.paypay.solution

import com.paypay.utils.ELBAccessLog
import com.paypay.utils.ELBAccessLog.{DATE_FORMATTER, DEFAULT_TZ}
import org.apache.spark.sql.Dataset

/**
  * Trait to provide the dataset for other test classes
  */
trait BaseSpec {
  def getTestDataset: Dataset[ELBAccessLog] = {
    // Creating a sample dataset for testing different use cases

    val seqElbAccessLog = Seq(
      ELBAccessLog(
        toMillis("2015-07-22T06:54:35.454767Z"),
        "49.15.32.186:9560",
        "10.0.4.244:81",
        "https://paytm.com:443/login",
        "Chrome/14.0.794.0 Safari/535.1",
        0L),
      ELBAccessLog(
        toMillis("2015-07-22T05:34:35.454767Z"),
        "49.15.32.186:9560",
        "10.0.4.244:80",
        "https://paytm.com:443/login",
        "Chrome/14.0.794.0 Safari/535.1",
        0L),
      ELBAccessLog(
        toMillis("2015-07-22T05:36:35.454767Z"),
        "49.15.32.186:9560",
        "10.0.4.244:80",
        "https://paytm.com:443/login",
        "Chrome/14.0.794.0 Safari/536.1",
        0L),
      ELBAccessLog(
        toMillis("2015-07-22T05:44:35.454767Z"),
        "49.15.32.186:9560",
        "10.0.4.244:80",
        "https://paytm.com:443/login",
        "Chrome/14.0.794.0 Safari/535.1",
        0L),
      ELBAccessLog(
        toMillis("2015-07-22T05:14:35.454767Z"),
        "49.15.32.186:9560",
        "10.0.4.244:80",
        "https://paytm.com:443/login",
        "Chrome/14.0.794.0 Safari/535.1",
        0L),

      ELBAccessLog(
        toMillis("2015-07-23T05:14:35.454767Z"),
        "59.15.32.184:9560",
        "10.0.4.243:80",
        "https://paytm.com:443/login",
        "Chrome/14.0.794.0 Safari/435.1",
        0L),
      ELBAccessLog(
        toMillis("2015-07-23T05:24:35.454767Z"),
        "59.15.32.184:9560",
        "10.0.4.243:81",
        "https://paytm.com:443/logout",
        "Chrome/14.0.794.0 Safari/435.1",
        0L),
      ELBAccessLog(
        toMillis("2015-07-23T05:34:35.454767Z"),
        "59.15.32.184:9560",
        "10.0.4.243:80",
        "https://paytm.com:443/search",
        "Chrome/14.0.794.0 Safari/435.1",
        0L),
      ELBAccessLog(
        toMillis("2015-07-23T05:44:35.454767Z"),
        "59.15.32.184:9560",
        "10.0.4.243:80",
        "https://paytm.com:443/logout",
        "Chrome/14.0.794.0 Safari/436.1",
        0L),
      ELBAccessLog(
        toMillis("2015-07-23T05:54:35.454767Z"),
        "59.15.32.184:9560",
        "10.0.4.243:80",
        "https://paytm.com:443/search",
        "Chrome/14.0.794.0 Safari/435.1",
        0L)

    )
    val spark = Helper.getSparkSession()
    import spark.implicits._
    spark.createDataset(seqElbAccessLog)
  }

  def toMillis(ts: String): Long = {
    DATE_FORMATTER.parseDateTime(ts).toDateTime(DEFAULT_TZ).getMillis
  }
}
