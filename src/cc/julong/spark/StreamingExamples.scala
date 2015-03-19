package cc.julong.spark

import org.apache.spark.Logging

import org.apache.log4j.{Level, Logger}

/**
 * Created by zhangfeng on 2015/2/11.
 */
object StreamingExamples {

  /** Set reasonable logging levels for streaming if the user has not configured log4j. */
  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}
