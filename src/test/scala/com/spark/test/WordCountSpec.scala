package com.spark.test

import org.apache.spark.{SparkContext, SparkConf}
import org.junit.Test

import scala.collection.mutable.ListBuffer

class WordCountSpec {

    @Test
    def test(){
        val conf = new SparkConf()
        conf.setMaster("local[*]").setAppName("WordCountTest")
        conf.set("spark.local.ip", "127.0.0.1")
        conf.set("spark.driver.host", "127.0.0.1")

        val sc = new SparkContext(conf)
        val input = new ListBuffer[String]
        input += "Apache Spark is a fast and general engine for large-scale data processing"
        input += "Spark runs on both Windows and UNIX-LIKE systems"
        input.toList

        val inputRDD = sc.parallelize(input)
        val resultRDD = WordCount.process(inputRDD)
        val resultMap = resultRDD.collectAsMap

        assert(resultMap("Spark") == 2)
        assert(resultMap("and") == 2)
        assert(resultMap("runs") == 1)

        println(resultMap)

        sc.stop
    }
}
