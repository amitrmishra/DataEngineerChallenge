package com.paypay.utils

import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTimeZone
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import scala.util.matching.Regex

/**
  * Class containing only those fields from the access log which are relevant for this exercise
  *
  * @param dateTime
  * @param clientAddress
  * @param backendAddress
  * @param requestedUrl
  * @param userAgent
  * @param sessionNum
  */
case class ELBAccessLog(dateTime: Long,
                        clientAddress: String,
                        backendAddress: String,
                        requestedUrl: String,
                        userAgent: String,
                        sessionNum: Long)

/**
  * Companion object to provide functionality of
  * filtering out malformed entries and getting an object of ELBAccessLog after parsing the log string
  */
object ELBAccessLog extends LazyLogging {
  // The regex to match the log entry against
  val LOG_REGEX: Regex = "^(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\" (\\S+) (\\S+)$".r
  val DATE_FORMATTER: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
  val DEFAULT_TZ: DateTimeZone = DateTimeZone.UTC

  def get(logLine: String): Option[ELBAccessLog] = {
    logLine match {
      case LOG_REGEX(dateTimeString, _, clientAddress, backendAddress, _, _, _, _, _, _, _, request, userAgent, _, _) =>
        Some(
          ELBAccessLog(
            DATE_FORMATTER.parseDateTime(dateTimeString).getMillis,
            clientAddress,
            backendAddress,
            request.split(' ')(1).split('?')(0), userAgent,
            0L
          )
        )
      case _ =>
        logger.warn(s"Could not parse record: $logLine")
        None
    }
  }
}