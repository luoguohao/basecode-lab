package com.luogh.learning.lab.flink

import java.nio.charset.StandardCharsets

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema

import scala.reflect._

class ValueWithMetaDeserializationSchema[A: ClassTag] extends KeyedDeserializationSchema[ValueWithMeta[A]] {
  override def isEndOfStream(nextElement: ValueWithMeta[A]): Boolean = false

  override def deserialize(messageKey: Array[Byte], message: Array[Byte], topic: String, partition: Int, offset: Long): ValueWithMeta[A] = {

    val key = Option(messageKey).map(new String(_, StandardCharsets.UTF_8.name()))
    val data = JSON.parseObject(new String(message, StandardCharsets.UTF_8.name()), classTag[A].runtimeClass)
    ValueWithMeta[A](offset, topic, partition, key, data.asInstanceOf[A])
  }

  override def getProducedType: TypeInformation[ValueWithMeta[A]] = TypeExtractor.getForClass(classOf[ValueWithMeta[A]])
}

case class ValueWithMeta[T](offset: Long, topic: String, partition: Int, messageKey: Option[String], data: T)