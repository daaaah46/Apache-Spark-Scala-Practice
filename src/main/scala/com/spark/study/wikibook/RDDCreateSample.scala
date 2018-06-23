package com.spark.study.wikibook

import org.apache.spark.{SparkConf, SparkContext}

object RDDCreateSample {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("RDDCreateSample")

        // RDD를 생성하기 위해서는 SparkContext를 생성해야 한다.
        val sc = new SparkContext(conf)

        // RDD를 생성하는 2가지 방법이 존재한다.
        // 1. 자바, 파이썬 : 리스트 타입, 스칼라 : 시퀀스 타입 객체를 사용하여 생성
        val rdd1 = sc.parallelize(List("a", "b", "c", "d", "e"))

        // 2. 파일과 같은 외부 데이터를 이용하는 방법
        val rdd2 = sc.textFile("<SPARK_HOME_DIR>/README.md")

        println(rdd1.collect.mkString(","))
        println(rdd2.collect.mkString(","))
    }
}
