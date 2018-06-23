package com.spark.study.wikibook

import org.apache.spark.{SparkConf, SparkContext}

class PassingFunctionSample {
    val count = 1

    def add(i:Int): Int = {
        count + i
    }

    // "main" org.apache.spark.SparkException: Task not serializable 에러 발생
    // 이 경우는 PassingFunctionSample 객체의 함수이기 때문에 (자바의 직렬화 규칙)에 따라 클래스 전체가 클러스터로 전달되야 하는 대상이 된 것
    def runMapSample(sc:SparkContext): Unit ={
        val rdd1 = sc.parallelize(1 to 10)
        val rdd2= rdd1.map(add)
        println(rdd2.collect())
    }

    // 위의 경우 다음과 같이 해결
    // Operation 이라는 이름의 싱글턴 객체를 하나 생성
    def runMapSample2(sc:SparkContext): Unit ={
        val rdd1 = sc.parallelize(1 to 10)
        val rdd2 = rdd1.map(Operations.add)
        print(rdd2.collect().toList) // List(2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
    }

    // java.io.NotSerializableException 에러 발생
    // 클래스가 아닌 멤버 변수에서도 전체 클래스를 직렬화 해야하는 상황이 일어난다.
    val increment = 1
    def runMapSample3(sc:SparkContext): Unit ={
        val rdd1 = sc.parallelize(1 to 10)
        val rdd2 = rdd1.map(_ + increment)
        print(rdd2.collect().toList)
    }

    // 위의 경우 지역변수로 변환해서 전달
    def runMapSample4(sc:SparkContext): Unit ={
        val rdd1 = sc.parallelize(1 to 10)
        val localIncrement = increment
        val rdd2 = rdd1.map(_ + localIncrement)
        println(rdd2.collect().toList)
    }
}

//싱글턴 객체 생성
object Operations {
    def add(i:Int) : Int = {
        i + 1
    }
}

object PassingFunctionSampleRunner{
    def main(args: Array[String]): Unit = {
        val sc = getSparkContext
        val sample = new PassingFunctionSample
        sample.runMapSample4(sc)
    }

    def getSparkContext(): SparkContext = {
        val conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("PassingFunctionSample")

        new SparkContext(conf)
    }
}

