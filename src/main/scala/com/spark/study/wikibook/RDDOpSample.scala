package com.spark.study.wikibook

import org.apache.spark.{SparkConf, SparkContext}

object RDDOpSample {

    def getSparkContext() : SparkContext = {
        val conf = new SparkConf()
        conf.setMaster("local[*]").setAppName("RDDOpSample")
        new SparkContext(conf)
    }

    def main(args: Array[String]): Unit = {
        val sc = getSparkContext()

        //doCollect(sc)
        //doCount(sc)
        //doMap(sc)
        //ex_2_18_flatMap(sc)
        //doFlatMap(sc)
        //ex_2_22_flatMap(sc)
        //doMapPartitions(sc)
        //doMapPartitionsWithIndex(sc)
        //doMapValues(sc)
        //doFlatMapValues(sc)
        //doFilter(sc)
        //doForeach(sc)
        doForeachPartition(sc)
    }

    // collect : rdd의 모든 원소를 모아서 배열로 돌려줌
    // 반환 타입은 배열
    // 대용량 데이터를 다룰 때는 성능상의 문제점을 고려한 뒤에 사용해야 함
    def doCollect(sc:SparkContext) {
        val rdd = sc.parallelize(1 to 10)
        val result = rdd.collect()
        println(result.mkString(",")) // 1,2,3,4,5,6,7,8,9,10
    }

    // count : rdd를 구성하는 전체 요소의 개수를 반환
    // 반환타입은 정수
    def doCount(sc:SparkContext) {
        val rdd = sc.parallelize(1 to 10)
        val result = rdd.count()
        println(result) // 10

    }

    // map :
    // 하나의 입력을 받아 하나의 값을 돌려주는 함수를 인자로 받음.
    // map 메서드는 이 함수를 RDD에 속하는 모든 요소에 적용한 뒤 그 결과로 구성된 새로운 rdd를 생성해 돌려줌
    // 반환타입은 RDD
    def doMap(sc:SparkContext) {
        val rdd = sc.parallelize(1 to 5)
        val result = rdd.map(_ + 1)
        println(result.collect().mkString(","))
    }

    // map과 flatmap의 차이는
    // map은 n개의 RDD가 n개로 결과가 나오고, flatmap은 n개의 rdd가 1개로 결과가 취합됨
    def ex_2_18_flatMap(sc:SparkContext): Unit ={
        // 단어 3개를 가진 List 생성
        val fruits = List("apple,orange", "grape,apple,mango", "blueberry,tomato,orange")
        // RDD 생성
        val rdd1 = sc.parallelize(fruits)
        // RDD의 map() 메서드로 각 단어를 "," 기준으로 분리 => rdd 3개 유지
        val rdd2 = rdd1.map(_.split(","))
        println(rdd2.collect().map(_.mkString("{", ", ", "}")).mkString("{", ", ", "}"))
    }

    // 반환 타입은 RDD
    def doFlatMap(sc:SparkContext): Unit ={
        val fruits = List("apple,orange","grape,apple,mango","blueberry,tomato,orange")
        val rdd1 = sc.parallelize(fruits)
        val rdd2 = rdd1.flatMap(_.split(","))
        println(rdd2.collect().mkString(","))
    }

    def ex_2_22_flatMap(sc:SparkContext): Unit ={
        val fruits = List("apple,orange", "grape,apple,mango", "blueberry,tomato,orange")
        val rdd1 = sc.parallelize(fruits)
        val rdd2 = rdd1.flatMap(log =>{
            // apple이라는 단어가 포함된 경우만 처리하고 싶다.
            if(log.contains("apple")){
                Some(log.indexOf("apple")) // some은 내부적으로 값이 있는 집합을 나타낸다
            } else {
                None // None은 내부적으로 값이 없는 집합을 나타낸다
            }
        })
        println(rdd2.collect().mkString(","))
    }

    // mapPartitions()는 파티션 단위로 처리하는 함수.
    // 전달 받은 함수를 파티션 단위로 적용하고, 그 결과로 구성된 새로운 RDD를 생성하는 메서드
    // 반환 타입은 RDD
    def doMapPartitions(sc:SparkContext): Unit ={
        val rdd1 = sc.parallelize(1 to 10, 3)
        val rdd2 = rdd1.mapPartitions(numbers => {
            println("Enter Partition!!!")
            numbers.map{
                number => number + 1
            }
        })
        println(rdd2.collect().mkString(","))
    }

    // mapPartitionsWithIndex()는 mapPartitions와 유사하지만
    // 인자로 전달되는 함수를 호출할 때 파티션에 속한 요소의 정보 뿐만 아니라 해당 파티션의 인덱스 정보도 함께 전달
    // 반환 타입은 RDD
    def doMapPartitionsWithIndex(sc:SparkContext): Unit ={
        val rdd1 = sc.parallelize(1 to 10, 3)
        // 첫번째 파티션에서만 결과를 추출하는 예제
        val rdd2 = rdd1.mapPartitionsWithIndex((idx, numbers) => {
            numbers.flatMap{
                case number if idx == 1 => Option(number+1)
                case _ => None  // default
            }
        }) // rdd2의 결과값은 첫번째 파티션에서 결과를 추출한 것
        println(rdd2.collect().mkString(",")) // 5,6,7 이지만 다를 수도 있음
    }


    // RDD의 요소가 키와 값의 쌍을 이루고 있는 경우, PairRDD라는 용어를 사용한다.
    // mapValues()는 RDD의 모든 요소들이 키와 값의 쌍을 이루고 있는 경우에만 사용 가능한 메서드 (PairRDD일 때 사용)
    // 인자로 전달받은 함수를 "값"에 해당하는 요소에만 적용, 그 결과로 구성된 새로운 RDD 생성
    // 반환 타입은 RDD
    def doMapValues(sc:SparkContext): Unit ={
        val rdd = sc.parallelize(List("a", "b", "c")).map((_,1)) // (a,1), (b,1), (c,1)
        val result = rdd.mapValues(i => i + 1) // value 값을 1 추가한다.
        println(result.collect().mkString("\t")) // (a,2)	(b,2)	(c,2)
    }

    // doMapValue와 마찬가지, 값에 해당하는 요소만을 대상으로 flatMap연산 적용
    // 반환 타입은 RDD
    def doFlatMapValues(sc : SparkContext): Unit ={
        val rdd = sc.parallelize(Seq((1, "a,b"), (2, "a,c"), (1, "d,e"))) // seq는 순서가 있는 list 형태, 3개의 RDD 생성
        val result = rdd.flatMapValues(_.split(","))
        println(result.collect().mkString("\t")) // (1,a)	(1,b)	(2,a)	(2,c)	(1,d)	(1,e)
    }

    // filter는 RDD의 요소 중에서 원하는 요소만 남기고 원하지 않는 요소는 걸러내는 동작을 하는 메서드
    // RDD의 어떤 요소가 원하는 조건에 부합하는지 여부를 참과 거짓으로 가려내는 함수를 RDD의 각 요소에 적용해 그 결과가 참인 것은 남기고 거짓인 것은 버림
    // 보통 Filter 연산은 처음 RDD를 만들고 나서 다른 처리를 수행하기 전에 불필요한 요소를 사전에 제거하는 목적
    // 반환 타입은 RDD
    def doFilter(sc : SparkContext): Unit ={
        val rdd = sc.parallelize(1 to 5)
        val result = rdd.filter(_ > 2)
        println(result.collect().mkString(", "))
    }

    // foreach()는 RDD의 모든 요소에 특정 함수를 적용하는 메서드
    // 반환타입 없음
    def doForeach(sc : SparkContext): Unit ={
        val rdd = sc.parallelize(1 to 10, 3)
        rdd.foreach(v =>
            println(s"Value Side Effect: ${v}")
        )
    }

    // foreachpartition()은 개별 요소가 아닌 파티션 단위로 특정 함수 적용
    // 반환타입 없음
    def doForeachPartition(sc: SparkContext): Unit ={
        val rdd = sc.parallelize(1 to 10, 3)
        rdd.foreachPartition(values => {
            println("Partition Side Effect!!")
            for (v <- values)
                println(s"Value Side Effect: ${v}")
        })
    }
}
