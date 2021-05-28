package com.paypay.utils

import com.paypay.solution.BaseSpec
import org.scalatest.FlatSpec

/**
  * Test class to check proper parsing of the web log strings
  */
class ELBAccessLogSpec extends FlatSpec with BaseSpec {

  /**
    * Test if well formed string is correctly parsed
    */
  "well-formed access log entry" must "be parsed correctly and return Option[Row] with correct types" in {
    // A sample well-formed string of web log entry
    val wellFormedLogLine = "2015-07-22T09:00:35.890581Z marketpalce-shop 106.66.99.116:37913 - -1 -1 -1 504 0 0 0 \"GET https://paytm.com:443/shop/orderdetail/1116223940?channel=web&version=2 HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.794.0 Safari/535.1\" ECDHE-RSA-AES128-SHA TLSv1"
    val optionOutput = ELBAccessLog.get(wellFormedLogLine)
    // Return type must be Some[ELBAccessLog]
    assert(optionOutput.isInstanceOf[Some[ELBAccessLog]])
    val elbAccessLog = optionOutput.get
    assert(elbAccessLog.dateTime.equals(toMillis("2015-07-22T09:00:35.890581Z")))
    assert(elbAccessLog.clientAddress.equals("106.66.99.116:37913"))
    assert(elbAccessLog.backendAddress.equals("-"))
    assert(elbAccessLog.requestedUrl.equals("https://paytm.com:443/shop/orderdetail/1116223940"))
    assert(elbAccessLog.userAgent.equals("Mozilla/5.0 (Windows NT 6.1) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.794.0 Safari/535.1"))
    assert(elbAccessLog.sessionNum.equals(0L))
  }

  /**
    * Test if malformed string is appropriately rejected
    */
  "mal-formed access log" must "return None" in {
    // A sample malformed string of web log entry
    val malFormedLogLine = "2015-07-22T16:10:38.028609Z marketpalce-shop 106.51.132.54:4841 10.0.4.227:80 0.000022 0.000989 0.00002 400 400 0 166 \"GET https://paytm.com:443/'\"\\'\\\");|]*{%0d%0a<%00>/about/ HTTP/1.1\" \"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)\" DHE-RSA-AES128-SHA TLSv1"
    // Return type must be None
    val optionOutput = ELBAccessLog.get(malFormedLogLine)
    assert(optionOutput == None)
  }
}