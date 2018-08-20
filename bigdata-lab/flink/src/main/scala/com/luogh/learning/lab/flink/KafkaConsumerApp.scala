package com.luogh.learning.lab.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer08, FlinkKafkaProducer08}

object KafkaConsumerApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.put("zookeeper.connect", "172.23.4.130:2181")
    properties.put("bootstrap.servers", "172.23.4.130:9092")

    val consumerProperties = new Properties(properties)
    consumerProperties.put("group.id", "test")
    consumerProperties.put("auto.offset.reset", "earliest")

    val dataSet = env.addSource(new FlinkKafkaConsumer08[String]("wiki-result", new SimpleStringSchema(), consumerProperties))
    val stream = dataSet.flatMap(x => x.split("\\W+"))
      .map(x => (x, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(1))
      .sum(1)
      .map(x => s"${x._1}-${x._2}")

    stream.addSink(new PrintSinkFunction[String]())
    stream.addSink(new FlinkKafkaProducer08[String]("wiki-result", new SimpleStringSchema(), properties))
    env.execute()
  }

}
