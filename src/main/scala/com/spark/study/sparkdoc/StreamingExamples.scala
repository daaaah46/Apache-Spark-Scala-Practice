package com.spark.study.sparkdoc

import org.apache.log4j.{Level, Logger}

object StreamingExamples {
    def setStreamingLogLevels() {
        val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
        if(!log4jInitialized){
            Logger.getRootLogger.setLevel(Level.WARN)
        }
    }
}
