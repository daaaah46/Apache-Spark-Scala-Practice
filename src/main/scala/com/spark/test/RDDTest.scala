package com.spark.test

import org.apache.spark.{SparkConf, SparkContext}

object RDDTest {
    def initSparkContext(){

    }

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
                .setAppName("RDD Test")
                .setMaster("local[3]")

        val sc = new SparkContext(conf)
        val numbers = sc.parallelize(10 to 100 by 10)

        numbers.foreach(x=>
            println(x)
        )

        val numbersSquared = numbers.map(num =>
            num * num
        )

        numbersSquared.foreach(x=>
            println(x)
        )

        numbersSquared.collect() // array로 반환
        numbersSquared.mean()
    }
}
