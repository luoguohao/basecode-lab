package com.luogh.learning.lab.flink

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, KeyedBroadcastProcessFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

object WindowWordCountApp2 {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val state = new MapStateDescriptor(
      "state",
      createTypeInformation[String],
      createTypeInformation[Int])

    val config = env.addSource(new SourceFunction[Int] {
      override def run(ctx: SourceFunction.SourceContext[Int]): Unit = {
        while (true) {
          Thread.sleep(1000)
          ctx.collect((math.random * 1000).toInt)
        }
      }

      override def cancel(): Unit = {}
    }).broadcast

    env.addSource(new SourceFunction[String] {
      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        while (true) {
          Thread.sleep(500)
          ctx.collect(s"a${(math.random * 10).toInt % 10}")
        }
      }

      override def cancel(): Unit = {}
    })
      .map(x => (x, 1))
      .keyBy(_._1)
      .mapWithState[(String, Int), Int] { case (tuple, state) =>
      state match {
        case Some(x) =>
          println(s"first, original state:${x}")
          (tuple, Some(x + 1))
        case None =>
          println("first, state not found ,init value : 0")
          (tuple, Some(0))
      }
    }.keyBy(_._1)
      .connect(config)
      .process(new CoProcessFunction[(String, Int), Int, (String, Int)] {
        override def processElement1(value: (String, Int), ctx: CoProcessFunction[(String, Int), Int, (String, Int)]#Context, out: Collector[(String, Int)]): Unit = ???

        override def processElement2(value: Int, ctx: CoProcessFunction[(String, Int), Int, (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {

        }
      })
      .keyBy(_._1)
      .countWindow(2)
      .process(new ProcessWindowFunction[(String, Int), (String, Int), String, GlobalWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
          println("=========== window ==============")
          getRuntimeContext.getMapState(state).iterator().asScala.foreach(entry => println(s">>>>>>>>>>> window broadcast entry: ${entry}"))
          context.globalState.getMapState(state).iterator().asScala.foreach(entry => println(s">>>>>>>>>>> window global state broadcast entry: ${entry}"))
          context.windowState.getMapState(state).iterator().asScala.foreach(entry => println(s">>>>>>>>>>> window window state broadcast entry: ${entry}"))
          elements.foreach(out.collect _)
        }
      })
//      .apply(new RichWindowFunction[(String, Int), (String, Int), String, GlobalWindow] {
//        override def apply(key: String, window: GlobalWindow, input: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
////          getRuntimeContext.getBroadcastVariable[Any](state.getName)
////            .listIterator().asScala
////            .foreach(entry => println(s"window broadcast entry: ${entry}"))
//          println("=========== window ==============")
//          getRuntimeContext.getMapState(state).iterator().asScala.foreach(entry => println(s"window broadcast entry: ${entry}"))
//          input.foreach(out.collect _)
//        }
//      })
      .print().setParallelism(1)

    env.execute("Window WordCount")
  }
}