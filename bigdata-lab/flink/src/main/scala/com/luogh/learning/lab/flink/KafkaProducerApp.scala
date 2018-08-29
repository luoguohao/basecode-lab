package com.luogh.learning.lab.flink

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource
import org.apache.logging.log4j.scala.Logging

object KafkaProducerApp extends Logging {

  def main(args: Array[String]): Unit = {
    logger.info("app started ...")
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.put("zookeeper.connect", "zk01.td.com:2181,zk02.td.com:2181,zk03.td.com:2181/kafka")
    properties.put("bootstrap.servers", "sz-pg-entps-dev-025.tendcloud.com:9092")
    env.addSource(new WikipediaEditsSource())
      .map(event => JSON.toJSONString(event, false))
      .addSink(new FlinkKafkaProducer010[String]("wiki-events-02", new SimpleStringSchema(), properties))

    env.execute()
  }

}
