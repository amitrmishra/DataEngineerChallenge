package com.paypay.solution

import com.paypay.solution.Helper.getSparkSession
import org.scalatest.FlatSpec

/**
  * Test class to check behavior of different data analysis tasks
  */
class DataAnalysisSpec extends FlatSpec with BaseSpec {
  /**
    * Testing the average session duration
    */
  "average session duration" must "return appropriate result" in {
    val spark = getSparkSession
    val sessionizedWebLogDs = SessionizeByWebLog.getSessionizedWebLog(getTestDataset, spark)
    val averageSessionDuration = DataAnalysis.getAverageSessionDurationPerUser(sessionizedWebLogDs, spark)
    // DataAnalysis.getAverageSessionDurationPerUser returns a dataframe.
    // The dataframe has user ip:port and userAgent and the average session duration of that user.
    val listAverageSessionDuration = averageSessionDuration.collect().sortBy(row => (row.getString(0), row.getString(1), row.getDouble(2)))
    assert(listAverageSessionDuration(0).getString(0).equals("49.15.32.186:9560")
      && listAverageSessionDuration(0).getString(1).equals("Chrome/14.0.794.0 Safari/535.1")
      && listAverageSessionDuration(0).getDouble(2).equals(200000.0))
    assert(listAverageSessionDuration(1).getString(0).equals("49.15.32.186:9560")
      && listAverageSessionDuration(1).getString(1).equals("Chrome/14.0.794.0 Safari/536.1")
      && listAverageSessionDuration(1).getDouble(2).equals(0.0))
    assert(listAverageSessionDuration(2).getString(0).equals("59.15.32.184:9560")
      && listAverageSessionDuration(2).getString(1).equals("Chrome/14.0.794.0 Safari/435.1")
      && listAverageSessionDuration(2).getDouble(2).equals(600000.0))
    assert(listAverageSessionDuration(3).getString(0).equals("59.15.32.184:9560")
      && listAverageSessionDuration(3).getString(1).equals("Chrome/14.0.794.0 Safari/436.1")
      && listAverageSessionDuration(3).getDouble(2).equals(0.0))
  }

  /**
    * Testing number of unique urls visited by a user per session
    */
  "number of unique urls per session" must "return appropriate result" in {
    val spark = getSparkSession
    val sessionizedWebLogDs = SessionizeByWebLog.getSessionizedWebLog(getTestDataset, spark)
    val numUniqueUrls = DataAnalysis.getNumUniqueUrlsPerSession(sessionizedWebLogDs, spark)
    // DataAnalysis.getNumUniqueUrlsPerSession returns a dataframe.
    // The dataframe has client ip:port, userAgent, sessionNum and number of unique urls visited by that user in that session.
    val listNumUniqueUrlsPerSession = numUniqueUrls.collect().sortBy(row => (row.getString(0), row.getString(1), row.getLong(2)))
    assert(listNumUniqueUrlsPerSession(0).getString(0).equals("49.15.32.186:9560")
      && listNumUniqueUrlsPerSession(0).getString(1).equals("Chrome/14.0.794.0 Safari/535.1")
      && listNumUniqueUrlsPerSession(0).getLong(2).equals(1L)
      && listNumUniqueUrlsPerSession(0).getLong(3).equals(1L))
    assert(listNumUniqueUrlsPerSession(1).getString(0).equals("49.15.32.186:9560")
      && listNumUniqueUrlsPerSession(1).getString(1).equals("Chrome/14.0.794.0 Safari/535.1")
      && listNumUniqueUrlsPerSession(1).getLong(2).equals(2L)
      && listNumUniqueUrlsPerSession(1).getLong(3).equals(1L))
    assert(listNumUniqueUrlsPerSession(2).getString(0).equals("49.15.32.186:9560")
      && listNumUniqueUrlsPerSession(2).getString(1).equals("Chrome/14.0.794.0 Safari/535.1")
      && listNumUniqueUrlsPerSession(2).getLong(2).equals(3L)
      && listNumUniqueUrlsPerSession(2).getLong(3).equals(1L))
    assert(listNumUniqueUrlsPerSession(3).getString(0).equals("49.15.32.186:9560")
      && listNumUniqueUrlsPerSession(3).getString(1).equals("Chrome/14.0.794.0 Safari/536.1")
      && listNumUniqueUrlsPerSession(3).getLong(2).equals(1L)
      && listNumUniqueUrlsPerSession(3).getLong(3).equals(1L))

    assert(listNumUniqueUrlsPerSession(4).getString(0).equals("59.15.32.184:9560")
      && listNumUniqueUrlsPerSession(4).getString(1).equals("Chrome/14.0.794.0 Safari/435.1")
      && listNumUniqueUrlsPerSession(4).getLong(2).equals(1L)
      && listNumUniqueUrlsPerSession(4).getLong(3).equals(3L))
    assert(listNumUniqueUrlsPerSession(5).getString(0).equals("59.15.32.184:9560")
      && listNumUniqueUrlsPerSession(5).getString(1).equals("Chrome/14.0.794.0 Safari/435.1")
      && listNumUniqueUrlsPerSession(5).getLong(2).equals(2L)
      && listNumUniqueUrlsPerSession(5).getLong(3).equals(1L))
    assert(listNumUniqueUrlsPerSession(6).getString(0).equals("59.15.32.184:9560")
      && listNumUniqueUrlsPerSession(6).getString(1).equals("Chrome/14.0.794.0 Safari/436.1")
      && listNumUniqueUrlsPerSession(6).getLong(2).equals(1L)
      && listNumUniqueUrlsPerSession(6).getLong(3).equals(1L))
  }

  /**
    * Testing the most engaged ips
    */
  "most engaged client ips" must "return appropriate result" in {
    val spark = getSparkSession
    val sessionizedWebLogDs = SessionizeByWebLog.getSessionizedWebLog(getTestDataset, spark)
    val mostEngagedUsers = DataAnalysis.getMostEngagedUsers(sessionizedWebLogDs, spark, Helper.NUM_MOST_ENGAGED_USERS)
    // DataAnalysis.getMostEngagedUsers returns a dataframe.
    // The dataframe has user ip:port, userAgent, session duration by that user and rank sorted in ascending order of rank.
    val listMostEngagedUsers = mostEngagedUsers.collect()
    assert(listMostEngagedUsers(0).getString(0).equals("59.15.32.184:9560")
      && listMostEngagedUsers(0).getString(1).equals("Chrome/14.0.794.0 Safari/435.1")
      && listMostEngagedUsers(0).getLong(2).equals(1200000L)
      && listMostEngagedUsers(0).getInt(3).equals(1))
    assert(listMostEngagedUsers(1).getString(0).equals("49.15.32.186:9560")
      && listMostEngagedUsers(1).getString(1).equals("Chrome/14.0.794.0 Safari/535.1")
      && listMostEngagedUsers(1).getLong(2).equals(600000L)
      && listMostEngagedUsers(1).getInt(3).equals(2))
  }
}
