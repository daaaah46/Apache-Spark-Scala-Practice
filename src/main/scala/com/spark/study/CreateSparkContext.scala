package com.spark.study

import org.apache.spark.{SparkConf, SparkContext}

object CreateSparkContext {
    def initializeSparkContext : SparkContext ={
        val conf = new SparkConf()
            .setAppName("RDD Test")
            .setMaster("local[3]")
        new SparkContext(conf)
    }

    def main(args: Array[String]): Unit = {
        val sc = initializeSparkContext
    }
}

