package com.smilegate.stove.test.streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
 실행 방법
 nc -lk 9000
 문자 입력
 */
object SocketSample {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[3]").setAppName("SocketSample")
        val ssc = new StreamingContext(conf, Seconds(3))

        val ds = ssc.socketTextStream("127.0.0.1", 9000)

        ds.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
