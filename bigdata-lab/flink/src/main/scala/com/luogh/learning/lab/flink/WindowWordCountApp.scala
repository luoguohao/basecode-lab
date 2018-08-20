package com.luogh.learning.lab.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._

object WindowWordCountApp {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.readTextFile("/Users/luogh/Code_Repository/luogh_repo/java_repo/basecode-lab/bigdata-lab/disruptor/logs/disruptor.log")
      .flatMap(x => x.split(" "))
      .map(x => (x, 1))
      .keyBy(0)
      .timeWindow(Time.minutes(1), Time.seconds(1))
      .reduce {
        (k1, k2) => (k1._1, k1._2 + k2._2)
      }.print().setParallelism(1)

    env.execute("Window WordCount")
  }
}