package com.luogh.learning.lab.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource
import org.apache.flink.streaming.api.scala._

object WikipediaAnalysis {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.addSource(new WikipediaEditsSource())
      .keyBy(x => x.getUser)
      .timeWindow(Time.seconds(5))
      .fold("" -> 0L)((foldV, event) => event.getUser -> (foldV._2 + event.getByteDiff))
      .print()

    env.execute()
  }

}
