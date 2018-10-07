package com.luogh.learning.lab.flink

import java.util.Properties

import com.alibaba.fastjson.JSON
import grizzled.slf4j.Logging
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent

import scala.util.Try


object BroadcastApp extends Logging {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(2000L)
    env.enableCheckpointing(Time.minutes(10).toMilliseconds, CheckpointingMode.EXACTLY_ONCE)
    val checkPointConfig = env.getCheckpointConfig
    checkPointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    val properties = new Properties()
    properties.put("bootstrap.servers", "sz-pg-entps-dev-025.tendcloud.com:9092")
    properties.put("group.id", "test6")
    properties.put("auto.offset.reset", "earliest")

    val stream = env.addSource(new FlinkKafkaConsumer010[String]("wiki-events", new SimpleStringSchema(), properties))
      .map(event =>
        JSON.parseObject(event, classOf[WikipediaEditEvent]))


    val config = env.addSource(new FlinkKafkaConsumer010[String]("wiki-configs", new SimpleStringSchema(), properties))
      .filter(!_.isEmpty)
      .map(event => Try(JSON.parseObject(event, classOf[Config])).getOrElse(Config("", 0)))


    stream.connect(config).map(new RichCoMapFunction[WikipediaEditEvent, Config, String] {

      var newestConfig: Config = _

      override def map2(value: Config): String = {
        this.newestConfig = value
        value.toString
      }

      override def map1(value: WikipediaEditEvent): String = {
        logger.info(s"with newest config:${this.newestConfig}")
        value.getUser
      }
    }).print()

    env.execute()
  }

  case class Config(k: String, v: Int)

}
