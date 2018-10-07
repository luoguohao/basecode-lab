package com.luogh.learning.lab.flink

import java.util.Properties

import grizzled.slf4j.Logging
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.json4s.ext.JavaTypesSerializers
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write
import org.json4s.{DefaultFormats, Formats, _}


object KafkaProducerApp extends Logging {

  implicit val formats: Formats = DefaultFormats.preservingEmptyValues ++ JavaTypesSerializers.all

  def main(args: Array[String]): Unit = {
    logger.info("app started ...")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val list = 4 to 20 map (User("test", _))
    val dataSet = env.fromCollection(list)
    val properties = new Properties()
    properties.put("zookeeper.connect", "zk01.td.com:2181,zk02.td.com:2181,zk03.td.com:2181/kafka")
    properties.put("bootstrap.servers", "sz-pg-entps-dev-025.tendcloud.com:9092")

    env.setParallelism(1)
    dataSet.map { user => write(user) }
      .addSink(new FlinkKafkaProducer010[String]("user-events", new SimpleStringSchema, properties))

    env.execute()
  }

  class UserIdPartitioner(partitionNum: Int) extends FlinkKafkaPartitioner[String] {

    override def partition(record: String, key: Array[Byte], value: Array[Byte], targetTopic: String, partitions: Array[Int]): Int = {
      parse(record).extract[User].user.hashCode % partitionNum
    }
  }

}
