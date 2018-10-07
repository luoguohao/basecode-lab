package com.luogh.learning.lab.flink

import java.util.Properties

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.luogh.learning.lab.flink.KafkaProducerApp.UserIdPartitioner
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010


object WikipediaAnalysis {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.put("bootstrap.servers", "sz-pg-entps-dev-025.tendcloud.com:9092")

    env.addSource(new WikipediaEditsSource())
      .map(x => JSON.toJSONString(x, SerializerFeature.WriteNullListAsEmpty))
      .addSink(new FlinkKafkaProducer010[String]("wiki-events", new SimpleStringSchema, properties))

    env.execute()
  }

}
