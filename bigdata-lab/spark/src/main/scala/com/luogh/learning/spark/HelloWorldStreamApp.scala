package com.luogh.learning.spark
import java.sql.Timestamp
import java.time.Instant

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object HelloWorldStreamApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Word Count")
      .getOrCreate()

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val words = lines.as[String].flatMap { strs ⇒
      val strArr = strs.split(" ")
      for (str <- strArr) yield {
        Event(str, Timestamp.from(Instant.now()))
      }
    }

    // Generate running word count
    val wordCounts = words
      .withWatermark("time", "1 minute")
      .groupBy(window($"time", "10 minutes", "5 minutes"), $"word")
      .count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}

case class Event(word: String, time: Timestamp)
