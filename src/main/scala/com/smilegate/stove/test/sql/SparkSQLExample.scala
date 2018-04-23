package com.smilegate.stove.test.sql

import org.apache.spark.sql.SparkSession

object SparkSQLExample {

    def main(args: Array[String]) {
        val spark = SparkSession
                .builder()
                .appName("Spark SQL basic Example")     // appName 설정
                .config("spark.some.config.option", "some-value")
                .master("local[*]")
                .getOrCreate()



    }
}
