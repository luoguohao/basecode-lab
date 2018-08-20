package com.luogh.learning.lab.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08

object KafkaProducerApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataSet = env.fromElements("To be, or not to be,--that is the question:--",
      "Whether 'tis nobler in the mind to suffer",
      "The slings and arrows of outrageous fortune",
      "Or to take arms against a sea of troubles,")

    val properties = new Properties()
    properties.put("zookeeper.connect", "172.23.4.130:2181")
    properties.put("bootstrap.servers", "172.23.4.130:9092")
    dataSet.flatMap(x => x.split("\\W+"))
      .map(x => (x, 1))
      .keyBy(0)
      .sum(1)
      .map(x => x._1 + x._2)
      .addSink(new FlinkKafkaProducer08[String]("wiki-result", new SimpleStringSchema(), properties))

    env.execute()
  }

}
