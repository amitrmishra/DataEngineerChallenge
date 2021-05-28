package com.paypay.solution

import com.paypay.solution.Helper._
import com.paypay.utils.ELBAccessLog
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

/**
  * Test class to check if sessionization of web logs is working as desired
  */
class SessionizeByWebLogSpec extends FlatSpec with BaseSpec {
  "test data set" must "be sessionized" in {
    val spark = getSparkSession
    val logsTestDs = getTestDataset

    // SessionizeByWebLog.getSessionizedWebLog returns a Dataset[ELBAccessLog]
    // It updates the field sessionNum to a long value
    // sessionNum is an incremental field same for a user session and different among different sessions of the same user.
    val sessionizeByWebLogDs = SessionizeByWebLog.getSessionizedWebLog(logsTestDs, spark)

    val listSessionizedWebLogs = sessionizeByWebLogDs.collect().sortBy(log => (log.clientAddress, log.userAgent, log.sessionNum))

    listSessionizedWebLogs should contain theSameElementsInOrderAs Seq(
      ELBAccessLog(
        toMillis("2015-07-22T05:14:35.454767Z"),
        "49.15.32.186:9560",
        "10.0.4.244:80",
        "https://paytm.com:443/login",
        "Chrome/14.0.794.0 Safari/535.1",
        1L),
      ELBAccessLog(
        toMillis("2015-07-22T05:34:35.454767Z"),
        "49.15.32.186:9560",
        "10.0.4.244:80",
        "https://paytm.com:443/login",
        "Chrome/14.0.794.0 Safari/535.1",
        2L),
      ELBAccessLog(
        toMillis("2015-07-22T05:44:35.454767Z"),
        "49.15.32.186:9560",
        "10.0.4.244:80",
        "https://paytm.com:443/login",
        "Chrome/14.0.794.0 Safari/535.1",
        2L),
      ELBAccessLog(
        toMillis("2015-07-22T06:54:35.454767Z"),
        "49.15.32.186:9560",
        "10.0.4.244:81",
        "https://paytm.com:443/login",
        "Chrome/14.0.794.0 Safari/535.1",
        3L),
      ELBAccessLog(
        toMillis("2015-07-22T05:36:35.454767Z"),
        "49.15.32.186:9560",
        "10.0.4.244:80",
        "https://paytm.com:443/login",
        "Chrome/14.0.794.0 Safari/536.1",
        1L),

      ELBAccessLog(
        toMillis("2015-07-23T05:14:35.454767Z"),
        "59.15.32.184:9560",
        "10.0.4.243:80",
        "https://paytm.com:443/login",
        "Chrome/14.0.794.0 Safari/435.1",
        1L),
      ELBAccessLog(
        toMillis("2015-07-23T05:24:35.454767Z"),
        "59.15.32.184:9560",
        "10.0.4.243:81",
        "https://paytm.com:443/logout",
        "Chrome/14.0.794.0 Safari/435.1",
        1L),
      ELBAccessLog(
        toMillis("2015-07-23T05:34:35.454767Z"),
        "59.15.32.184:9560",
        "10.0.4.243:80",
        "https://paytm.com:443/search",
        "Chrome/14.0.794.0 Safari/435.1",
        1L),
      ELBAccessLog(
        toMillis("2015-07-23T05:54:35.454767Z"),
        "59.15.32.184:9560",
        "10.0.4.243:80",
        "https://paytm.com:443/search",
        "Chrome/14.0.794.0 Safari/435.1",
        2L),
      ELBAccessLog(
        toMillis("2015-07-23T05:44:35.454767Z"),
        "59.15.32.184:9560",
        "10.0.4.243:80",
        "https://paytm.com:443/logout",
        "Chrome/14.0.794.0 Safari/436.1",
        1L)
    )
  }
}
