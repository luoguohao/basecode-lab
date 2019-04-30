package com.luogh.learning.lab.flink

import java.util.Properties

import com.alibaba.fastjson.JSON
import grizzled.slf4j.Logging
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ReducingStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.heap.{HeapReducingState, HeapValueState}
import org.apache.flink.runtime.state.internal.InternalValueState
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent
import org.apache.flink.util.Collector


object KafkaConsumerWithWindowPreAggApp extends Logging {

  def main(args: Array[String]): Unit = {
    logger.info("application start ...")

    val windowRange = ParameterTool.fromArgs(args).getLong("sessionWindowSizeInMinute", 1)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(2000L)
    env.enableCheckpointing(Time.minutes(10).toMilliseconds, CheckpointingMode.EXACTLY_ONCE)
    val checkPointConfig = env.getCheckpointConfig
    checkPointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)

    val properties = new Properties()
    properties.put("bootstrap.servers", "sz-pg-entps-dev-025.tendcloud.com:9092")
    properties.put("group.id", "test12")
    properties.put("auto.offset.reset", "earliest")

    env.setParallelism(1)
    env.setMaxParallelism(1)

    val kafkaConsumer = new FlinkKafkaConsumer010[String]("user-events", new SimpleStringSchema(), properties).setStartFromTimestamp(0)

    val stream = env.addSource(kafkaConsumer)
      .map(event => JSON.parseObject(event, classOf[User]))
      .assignTimestampsAndWatermarks(new TimestampWaterMarker(
        Time.milliseconds(5).toMilliseconds,
        Time.milliseconds(30).toMilliseconds,
        Time.milliseconds(2).toMilliseconds
      )) // 允许10秒乱序，watermark为当前接收到的最大事件时间戳减10秒
      .setParallelism(1)
      .keyBy(_.user)
      .window(EventTimeSessionWindows.withGap(Time.milliseconds(10)))
      //      .allowedLateness(Time.seconds(5))
      //      .sideOutputLateData(OutputTag[WikipediaEditEvent]("late_data"))
      //      .trigger(EventTimeTrigger.create())
      //      .trigger(EarlyTriggeringTrigger.every(Time.seconds(10)))
      .trigger(new WindowStartEndTrigger())
      .aggregate(new AggregateFunction[User, (String, Long), (String, Long)] {

        override def add(in: User, acc: (String, Long)): (String, Long) = {
          logger.debug(s"aggregate add opt:${in}, acc:${acc}")
          (in.user, acc._2 + 1)
        }

        override def createAccumulator(): (String, Long) = ("", 0)

        override def getResult(acc: (String, Long)): (String, Long) = acc

        override def merge(acc: (String, Long), acc1: (String, Long)): (String, Long) = {
          logger.debug(s"aggregate merge opt: acc1:${acc}, acc2:${acc1} ")
          (acc._1, acc._2 + acc1._2)
        }
      }, new ProcessWindowFunction[(String, Long), (String, Long), String, TimeWindow] {

        private val triggerTypeStat = new ValueStateDescriptor[TriggerType]("triggerTypeStat", createTypeInformation[TriggerType])

        private val min: ReduceFunction[Long] = new ReduceFunction[Long] {
          override def reduce(value1: Long, value2: Long): Long = {
            logger.info(s"reduce operator =====> value1:${value1}, value2:${value2}")
            math.min(value1, value2)
          }
        }

        val stateDesc = new ReducingStateDescriptor[Long]("fire-time", min, createTypeInformation[Long])
        //这里取2个注册时间的最小值，因为首先注册的是窗口的maxTimestamp，也是最后一个要触发的时间


        override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[(String, Long)]): Unit = {
          val window = context.window
          val sessionId = generateSessionId(key, window.getStart)
          logger.debug(s"window process opt => key:${key}, " +
            s"${elements.seq.map(_._2).mkString(",")} for window:${window}" +
            s" with session id:${sessionId}")

          elements.foreach(out.collect _)

          // because merging window is not permitted to access per-window state for now, so in order
          // to access the state stored in Trigger context for a arbitrary window, Currently to break
          // this limitation, a hack within the WindowFunction is effective, but not elegant right now.

          val stat = context.globalState.getState(triggerTypeStat) match {
            case state: InternalValueState[
              String @unchecked,
              TimeWindow @unchecked,
              TriggerType @unchecked
              ] =>
              state.setCurrentNamespace(window)
              state.value()
            case _ @ unsupportedState =>
              throw new RuntimeException(s"unsupported value state:${unsupportedState} right now, check.")
          }
          logger.info(s"process(key=${key}, window=${context.window}) ===== triggerType:${stat}")

          val result = stat
          logger.info(s"process(key=${key}, window=${context.window}) ===== state:${result}")
        }
      }).uid("windowSessionFunction") // uid for the checkpoint & savepoint

    stream.writeUsingOutputFormat(HbaseOutputFormat("wiki-event", new Configuration()))

    stream.getSideOutput(OutputTag[WikipediaEditEvent]("late_data"))
      .map(x => s"late_data:${x.getUser}")
      .map(_.toString)
      .addSink(new BucketingSink[String]("/tmp/logs/result_late_data"))

    env.execute()
  }

  /**
    * Attention: session window will merge all the relevant windows together, so if we wanna
    * a unique session id for a user we just using the session start time except the session end
    * time, because session end time is still incorrect when session start event time trigger the
    * window processor
    *
    * @param uid
    * @param sessionStartTime
    * @return
    */
  def generateSessionId(uid: String, sessionStartTime: Long): String = {
    s"${uid}_${sessionStartTime}"
  }
}
