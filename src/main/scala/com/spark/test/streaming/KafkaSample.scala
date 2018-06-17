package com.spark.test.streaming

import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods.parse

object KafkaSample {
    def main(args: Array[String]){
        val conf = new SparkConf()
                .setMaster("local[3]")
                .setAppName("KafkaSample")

        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(3))

        ssc.start()
        ssc.awaitTermination()
    }
}