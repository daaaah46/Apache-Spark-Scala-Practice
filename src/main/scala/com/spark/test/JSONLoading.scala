package com.spark.test

import org.apache.spark.sql.SparkSession

object JSONLoading {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
                .builder()
                .appName("GitHub Push counter")
                .master("local[*]")
                .getOrCreate()
        val sc = spark.sparkContext

    }
}
