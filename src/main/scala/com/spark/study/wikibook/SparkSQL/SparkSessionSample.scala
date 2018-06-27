package com.spark.study.wikibook.SparkSQL

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSessionSample {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession
                .builder()
                .appName("Sample")
                .master("local[*]")
                .getOrCreate()

        val source = "/Users/user/Applications/spark/README.md"
        val df = spark.read.text(source)

        runUntypedTransformationsExample(spark, df)
        runTypedTransformationsExample(spark, df)
    }

    def runUntypedTransformationsExample(spark: SparkSession, df: DataFrame): Unit ={
        import org.apache.spark.sql.functions._

        df.show()
//      +--------------------+
//      |               value|
//      +--------------------+
//      |      # Apache Spark|
//      |                    |
//      |Spark is a fast a...|
//      |high-level APIs i...|
//      |supports general ...|
//      |rich set of highe...| 이러한 형태

        // value라는 컬럼을 가져와서(col("value")) 컬럼에 포함된 요소를 여러개의 행으로 변환하고 컬럼 명을 word로 변경 as("word")
        val wordDF = df.select(explode(split(col("value"), " ")).as("word"))
        wordDF.show()

        val result = wordDF.groupBy("word").count()
        result.show()

    }

    def runTypedTransformationsExample(spark: SparkSession, df: DataFrame): Unit ={

        import spark.implicits._

        val ds = df.as[(String)] // 데이터프레임을 데이터 셋으로 변환하는 부분
        val wordDF = ds.flatMap(_.split(" "))
        val result = wordDF.groupByKey(v => v).count()

        result.show()
    }

}
