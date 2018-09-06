package com.luogh.learning.lab.flink

import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.logging.log4j.scala.Logging

object StateApp extends Logging {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(2000L)
    env.enableCheckpointing(Time.minutes(10).toMilliseconds, CheckpointingMode.EXACTLY_ONCE)
    val checkPointConfig = env.getCheckpointConfig
    checkPointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    env.fromCollection(1 to 100 zip (1 to 100))
      .keyBy(_._1)
      .filterWithState { (tuple, state: Option[String]) => (tuple._1 > 0, Option("ste")) }
      .keyBy(_._1)
      .mapWithState((in: (Int, Int), count: Option[Int]) =>
        count match {
          case Some(c) => ((in._1, c), Some(c + in._2))
          case None => ((in._1, 0), Some(in._2))
        })

      .print()

    env.execute()
  }
}
