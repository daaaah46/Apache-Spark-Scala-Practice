package com.spark.test.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HdfsWordCount {
    def main(args: Array[String]){
        if (args.length < 1){
            System.err.println("Usage : HdfsWordCount <directory>")
            System.exit(1)
        }

        StreamingExamples.setStreamingLogLevels()
        val sparkConf = new SparkConf().setAppName("HdfsWordCount")
        val ssc = new StreamingContext(sparkConf, Seconds(2))

        val lines = ssc.textFileStream(args(0))
        val words = lines.flatMap(_.split(" "))
        val wordCounts = words.map(x => (x, 1)).reduceByKey(_+_)
        wordCounts.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
