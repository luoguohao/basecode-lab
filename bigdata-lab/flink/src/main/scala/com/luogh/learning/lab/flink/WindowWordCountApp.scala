package com.luogh.learning.lab.flink

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

object WindowWordCountApp {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val state = new MapStateDescriptor(
      "state",
      createTypeInformation[String],
      createTypeInformation[Int])

    val config = env.addSource(new SourceFunction[(Int, Int)] {
      override def run(ctx: SourceFunction.SourceContext[(Int, Int)]): Unit = {
        while (true) {
          Thread.sleep(1000)
          ctx.collect(((math.random * 1000).toInt -> 1))
        }
      }

      override def cancel(): Unit = {}
    }).keyBy(_._1).map(new RichMapFunction[(Int, Int), Int] {
      override def map(value: (Int, Int)): Int = {
        val newV = value._1 + 1
        getRuntimeContext.getMapState(state).put("a", newV)
        newV
      }
    }).broadcast(state)

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
      .process(new KeyedBroadcastProcessFunction[String, (String, Int), Int, (String, Int)] {
        override def processElement(value: (String, Int), ctx: KeyedBroadcastProcessFunction[String, (String, Int), Int, (String, Int)]#ReadOnlyContext, out: Collector[(String, Int)]): Unit = {
          println(s"receive ELement:${value}")
          val s = ctx.getBroadcastState(state)
          s.immutableEntries().iterator().asScala.foreach { entry => println(s"broadcast entry: ${entry}") }
          out.collect(value)
        }

        override def processBroadcastElement(value: Int, ctx: KeyedBroadcastProcessFunction[String, (String, Int), Int, (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
          println(s"receive broadcast ELement:${value} in thread:${Thread.currentThread().getName}")
          val s = ctx.getBroadcastState(state)
          s.iterator().asScala.foreach { entry => println(s"---------------broadcast value: ${entry} in thread:${Thread.currentThread().getName}") }
          s.put("a", value)
        }
      }).keyBy(_._1)
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