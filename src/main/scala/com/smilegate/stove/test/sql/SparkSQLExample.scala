package com.smilegate.stove.test.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object SparkSQLExample {

    val _jsonpath = "src/main/resources/people.json"
    val _textpath = "src/main/resources/people.txt"

    // CaseClass 선
    case class Person(name: String, age : Long)

    def main(args: Array[String]) {
        val sparkContext = new SparkContext()
        // 데이터 프레임을 생성하기 위해서는 SparkSession 을 이용해야 함
        // Spark SQL 은 가장 먼저 SparkSession을 생성하는 것으로 부터 시작
        // SparkSession 은 Spark 2.0 부터 시작, SparkSession = SQLContext + HiveContext
        val spark = SparkSession
                .builder()
                .appName("Spark SQL basic Example")     // appName 설정
                .config("spark.some.config.option", "some-value")       // 추가적으로 설정이 필요할 때 사용
                .master("local[*]")
                .getOrCreate()

        //runBasicDataFrameExample(spark)
        //runDatasetCreationExample(spark)
        //runInferSchemaExample(spark)
        //runProgrammaticSchemaExample(spark)
        spark.stop()
    }

    private def runBasicDataFrameExample(spark: SparkSession): Unit ={
        // json 파일을 읽어와서 데이터 프레임을 생성한다.
        val df = spark.read.json(_jsonpath)

        // 데이터 프레임의 내용을 출력
        df.show()
        /*
        +----+-------+
        | age|   name|
        +----+-------+
        |null|Michael|
        |  30|   Andy|
        |  19| Justin|
        +----+-------+
         */

        // 스키마 정보를 출력
        df.printSchema()
        /*
        root
         |-- age: long (nullable = true)
         |-- name: string (nullable = true)
         */

        // name 칼럼만을 출력
        df.select("name").show()
        /*
        +-------+
        |   name|
        +-------+
        |Michael|
        |   Andy|
        | Justin|
        +-------+
         */

        // This import is needed to use the $-notation > 암묵적 변환 사용
        import spark.implicits._

        // 특정 컬럼을 가져와서 연산 후 출력
        // $ 는 열을 선택하고, 그 위에 함수를 적용한다는 의미, $ 없으면 에러
        df.select($"age"+1).show()
        /*
        +-------+---------+
        |   name|(age + 1)|
        +-------+---------+
        |Michael|     null|
        |   Andy|       31|
        | Justin|       20|
        +-------+---------+
         */

        // age가 21보다 큰 사람을 출력
        df.filter($"age" > 21).show()
        /*
        +---+----+
        |age|name|
        +---+----+
        | 30|Andy|
        +---+----+
        */


        // age 컬럼으로 묶어서 카운트한 뒤 출력
        df.groupBy("age").count().show()
        /*
        +----+-----+
        | age|count|
        +----+-----+
        |  19|    1|
        |null|    1|   // null은 카운트 안하고 싶을 땐 어쩌지?
        |  30|    1|
        +----+-----+
         */

        // Register the DataFrame as a SQL temporary view
        // 데이터프레임을 테이블처럼 SQL을 사용해서 처리할 수 있게 등록해줌
        // 단, 이 메서드로 생성된 테이블은 스파크 세션이 유지되는 동안만 유효히고, 해당 세션이 종료되면 사라짐
        df.createOrReplaceTempView("people")
        spark.sql("select * from people").show()
        /*
        +----+-------+
        | age|   name|
        +----+-------+
        |null|Michael|
        |  30|   Andy|
        |  19| Justin|
        +----+-------+
         */


        // Register the DataFrame as a global temporary view
        // createOrReplaceTempView() 은 스파크 세션이 종료되면 사라진다.
        // createGlobalTempView()의 경우 Global Temporary View로 등록하여
        // 스파크 어플리케이션이 종료되기 전 까지 모든 세션 사이에서 공유될 수 있다.
        // Global Temporary View의 경우 시스템 DB인 global_temp 에 연결되어 있기 때문에
        // global_temp.등록변수이름 으로 사용해야 한다
        df.createGlobalTempView("people")
        spark.sql("SELECT * FROM global_temp.people").show()
        /*
        +----+-------+
        | age|   name|
        +----+-------+
        |null|Michael|
        |  30|   Andy|
        |  19| Justin|
        +----+-------+
         */

        // 새로운 세션에서도 Global Temporary View 로 등록한 people 호출 가능
        spark.newSession().sql("SELECT * FROM global_temp.people").show()
        /*
        +----+-------+
        | age|   name|
        +----+-------+
        |null|Michael|
        |  30|   Andy|
        |  19| Justin|
        +----+-------+
         */
    }

    // DataSet 실습
    private def runDatasetCreationExample(spark: SparkSession): Unit ={
        import spark.implicits._

        // Seq 기반으로 Dataset 생성
        val caseClassDS = Seq(Person("Andy", 32), Person("Justin", 19)).toDS()
        caseClassDS.show()
        /*
        +-------+---+
        |   name|age|
        +-------+---+
        |   Andy| 32|
        | Justin| 19|
        +-------+---+
         */

        // Seq 기반으로 Dataset 생성, 이 경우 스키마 x
        val primitiveDS = Seq(1,2,3).toDS()
        primitiveDS.map(_ + 1).collect().foreach(println)

        //
        val peopleDS = spark.read.json(_jsonpath).as[Person]
        peopleDS.show()
        /*
        +----+-------+
        | age|   name|
        +----+-------+
        |null|Michael|
        |  30|   Andy|
        |  19| Justin|
        +----+-------+
         */
    }

    // Inferring the Schema Using Reflection
    private def runInferSchemaExample(spark: SparkSession): Unit ={
        import spark.implicits._

        // 텍스트 파일을 읽어와서 Person 형태 RDD를 생성한 뒤, 데이터 프레임으로 변경
        val peopleDF = spark.sparkContext
                .textFile(_textpath)        // 여기까지가 RDD 생성하는 코
                .map(_.split(","))
                .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
                .toDF()

        // 데이터 프레임을 temporary view 로 등록
        peopleDF.createOrReplaceTempView("people")

        // Spark 에서 제공되는 SQL 메소드를 사용하여 쿼리를 실행
        val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19 ")

        // 쿼리 결과 중 하나의 열에 대한 컬럼은 인덱스 값이나 필드 이름으로 접근할 수 있음
        teenagersDF.map(teenager => "Name:" + teenager(0)).show()
        teenagersDF.map(teenager => "Name:" + teenager.getAs[String]("name")).show()
        /*
        +-----------+
        |      value|
        +-----------+
        |Name:Justin|
        +-----------+
         */

        // scala-implicit 알아보고 추가
    }

    // Programmatically Specifying the Schema
    private def runProgrammaticSchemaExample(spark: SparkSession): Unit = {
        import spark.implicits._

        // RDD를 생성한다.
        val peopleRDD = spark.sparkContext.textFile(_textpath)

        // String 으로 스키마 인코딩
        val schemaString = "name age"

        // String 형태로 선언된 것을 바탕으로 스키마를 생성한다.
        // j
        val fields = schemaString.split(" ")
                .map(fieldName => StructField(fieldName, StringType, nullable = true))
        //val schema = StringType(fields)

    }
}