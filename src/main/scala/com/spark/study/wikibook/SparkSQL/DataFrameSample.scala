package com.spark.study.wikibook.SparkSQL

import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}

object DataFrameSample {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder()
                .appName("DataFrameSample")
                .master("local[*]")
                .config("spark.driver.host", "127.0.0.1")
                .getOrCreate()

        val sc = spark.sparkContext

        import spark.implicits._

        val row1 = Person("hayoon", 7, "student")
        val row2 = Person("sunwoo", 13, "student")
        val row3 = Person("hajoo", 5, "kindergartener")
        val row4 = Person("jinwoo", 23, "student")
        val data = List(row1, row2, row3, row4)
        val sampleDF = spark.createDataFrame(data)

        val d1 = ("store2", "note", 20, 2000)
        val d2 = ("store2", "bag", 10, 5000)
        val d3 = ("store1", "note", 15, 1000)
        val d4 = ("store1", "pen", 20, 5000)
        val sampleDF2 = Seq(d1, d2, d3, d4).toDF("store", "product", "amount", "price")

        val ldf = Seq(Word("w1", 1), Word("w2", 1)).toDF()
        val rdf = Seq(Word("w1", 2), Word("w3", 1)).toDF()

        //createDataFrame(spark, spark.sparkContext)
        //runBasicOpsEx(spark, sc, sampleDF)
        //runColumnEx(spark,sc,sampleDF)
        runWithColumn(spark)

    }

    def createDataFrame(spark: SparkSession, sc: SparkContext): Unit ={

        import spark.implicits._

        val sparkHomeDir = "/Users/user/Applications/spark"

        // 1. 파일로부터 생성
        val df1 = spark.read.json(sparkHomeDir + "/examples/src/main/resources/people.json")
        val df2 = spark.read.parquet(sparkHomeDir + "/examples/src/main/resources/users.parquet")
        val df3 = spark.read.text(sparkHomeDir + "/examples/src/main/resources/people.txt")

        val row1 = Person("hayoon", 7, "student")
        val row2 = Person("sunwoo", 13, "student")
        val row3 = Person("hajoo", 5, "kindergartener")
        val row4 = Person("jinwoo", 13, "student")
        val data = List(row1, row2, row3, row4)
        val df4 = spark.createDataFrame(data)
        df4.show

        val df5 = data.toDF()

        val rdd = sc.parallelize(data)
        val df6 = spark.createDataFrame(rdd)
        val df7 = rdd.toDF()

        val sf1 = StructField("name", StringType, nullable = true)
        val sf2 = StructField("age", IntegerType, nullable = true)
        val sf3 = StructField("job", StringType, nullable = true)
        val schema = StructType(List(sf1, sf2, sf3))

        val rows = sc.parallelize(List(Row("hayoon", 7, "student"), Row("sunwoo", 13, "student"),
            Row("hajoo", 5, "kindergartener"), Row("jinwoo", 13, "student")))
        val df8 = spark.createDataFrame(rows, schema)
        df8.show
    }

    def runBasicOpsEx(spark: SparkSession, sc: SparkContext, df : DataFrame): Unit ={
        df.show() // 데이터셋에 저장된 데이터를 화면에 출력해서 보여줌
        df.head() // 데이터셋의 첫 번째 로우를 돌려줌
        df.first() // head()와 같음
        df.take(2) // 데이터셋의 첫 n개의 로우를 돌려줌
        df.count()
        df.collect()
        df.collectAsList()
        df.describe("age").show()
        df.printSchema()
        df.columns
        df.createOrReplaceTempView("users")
        spark.sql("select name, age from users where age > 20").show()
        spark.sql("select name, age from users where age > 20").explain()
    }

    def runColumnEx(spark: SparkSession, sc: SparkContext, df: DataFrame): Unit ={

        import spark.implicits._

        df.createOrReplaceTempView("person")
        spark.sql("select * from person where age > 10 ").show

        df.where(df("age") > 10).show
        df.where('age > 10).show
        df.where($"age" > 10).show

    }

    def runAlias(spark: SparkSession, sc: SparkContext, df: DataFrame): Unit ={

        import spark.implicits._

        df.select('age + 1).show()
        df.select(('age + 1).as("age")).show()

        val df1 = List(MyCls("id1", Map("key1" -> "value1", "key2" -> "value2"))).toDF("id", "value")

    }

    def runWithColumn(spark: SparkSession): Unit ={

        import spark.implicits._

        val df1 = List(("prod1", "100"), ("prod2", "200")).toDF("pname", "price")
        val df2 = df1.withColumn("dcprice", 'price * 0.9)
        val df3 = df2.withColumnRenamed("dcprice", "newprice") // dcprice 컬럼을 newprice 로 변경

        df1.show()
        df2.show()
        df3.show()
    }
}
