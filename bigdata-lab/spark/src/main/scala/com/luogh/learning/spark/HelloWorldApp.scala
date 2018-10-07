package com.luogh.learning.spark

import org.apache.spark.sql.SparkSession

object HelloWorldApp {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Word Count")
      .getOrCreate()

    val rdd = sparkSession.sparkContext.makeRDD(1 to 100)

    import sparkSession.implicits._
    rdd.toDF().show()

    sparkSession.sparkContext.makeRDD(1 to 100).zipWithUniqueId().foreach(println _)
  }

}
