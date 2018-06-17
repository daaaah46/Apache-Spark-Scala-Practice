package com.spark.test.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

object WordCount {

    def main(args: Array[String]): Unit = {

        /*
         * step1
         * 스파크 SQL을 다루기 위한 스파크 세션을 생성하는 부분
         */
        val spark = SparkSession
                .builder()
                .appName("WordCount")
                .master("local[*]")
                .config("spark.local.ip", "127.0.0.1")
                .config("spark.driver.host", "127.0.0.1")
                .getOrCreate()

        // step2
        import  spark.implicits._
        import org.apache.spark.sql.functions._

        // step3
        val lines = spark
                .readStream
                .format("socket")
                .option("host","localhost")
                .option("port", 9999)
                .load()


        // step4
        val words = lines.select(explode(split('value," ")).as("word"))
        val wordCount = words.groupBy("word").count

        // step5
        val query = wordCount.writeStream
                .outputMode(OutputMode.Complete)
                .format("console")
                .start()

        // step6
        query.awaitTermination()
    }
}
