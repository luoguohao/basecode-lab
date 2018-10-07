package com.luogh.learning.lab.flink

import grizzled.slf4j.Logging
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.ReducingStateDescriptor
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.typeutils.base.LongSerializer
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent

class EarlyTriggeringTrigger(interval: Long) extends Trigger[WikipediaEditEvent, TimeWindow] with Logging {

    //通过reduce函数维护一个Long类型的数据，此数据代表即将触发的时间戳
    private type JavaLong = java.lang.Long

    //这里取2个注册时间的最小值，因为首先注册的是窗口的maxTimestamp，也是最后一个要触发的时间
    private val min: ReduceFunction[JavaLong] = new ReduceFunction[JavaLong] {
      override def reduce(value1: JavaLong, value2: JavaLong): JavaLong = {
        logger.info(s"reduce operator =====> value1:${value1}, value2:${value2}")
        Math.min(value1, value2)
      }
    }


    private val serializer: TypeSerializer[JavaLong] = LongSerializer.INSTANCE.asInstanceOf[TypeSerializer[JavaLong]]

    private val stateDesc = new ReducingStateDescriptor[JavaLong]("fire-time", min, serializer)

    /**
      * 之前注册的Event Time Timer定时器，当watermark超过注册的时间时，就会执行onEventTime方法
      */
    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      // 如果注册的时间等于maxTimestamp时间，清空状态，并触发计算
      if (time == window.maxTimestamp()) {
        clearTimerForState(ctx)
        logger.debug(s"onEventTime trigger At window end ====> time:${time},window:${window}")
        TriggerResult.FIRE
      } else {
        // 否则，获取状态中的值（maxTimestamp和nextFireTimestamp的最小值）
        val fireTimestamp = ctx.getPartitionedState(stateDesc)
        // 如果状态中的值等于注册的时间，则删除此定时器时间戳，并注册下一个interval的时间，触发计算
        // 这里的前提条件是watermark超过了定时器中注册的时间，就会执行此方法，理论上状态中的fire time 一定是等于注册的时间的
        if (fireTimestamp.get() == time) {
          fireTimestamp.clear()
          fireTimestamp.add(time + interval)
          ctx.registerEventTimeTimer(time + interval)
          logger.debug(s"onEventTime trigger At event trigger interval ====> time:${time},window:${window}")
          TriggerResult.FIRE
        } else {
          TriggerResult.CONTINUE
        }
      }
    }

    //这里不基于processing time，因此永远不会基于processing time 触发
    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    // 删除定时器中已经触发的时间戳，并调用Trigger的clear方法
    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
      logger.debug(s"clear ====> window:${window}")
      ctx.deleteEventTimeTimer(window.maxTimestamp())
      val fireTimestamp = ctx.getPartitionedState(stateDesc)
      val timestamp = fireTimestamp.get()
      if (timestamp != null) {
        ctx.deleteEventTimeTimer(timestamp)
        fireTimestamp.clear()
      }
    }

    override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {
      logger.debug(s"onMerge ====> window:${window}")
      ctx.mergePartitionedState(stateDesc)
      val nextFireTimestamp = ctx.getPartitionedState(stateDesc).get()
      if (nextFireTimestamp != null) {
        ctx.registerEventTimeTimer(nextFireTimestamp)
      }
    }

    // 用于session window的merge,判断是否可以merge
    override def canMerge: Boolean = true

    //每个元素都会运行此方法
    override def onElement(element: WikipediaEditEvent, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      // 如果当前的watermark超过窗口的结束时间，则清除定时内容，触发窗口计算
      if (window.maxTimestamp <= ctx.getCurrentWatermark) {
        clearTimerForState(ctx)
        logger.debug(s"onElement trigger At window end ====> currentWatermark:${ctx.getCurrentWatermark},window:${window}")
        TriggerResult.FIRE
      } else {
        logger.debug(s"onElement trigger for each element come ====> currentWatermark:${ctx.getCurrentWatermark},window:${window}")
        // 否则将窗口的结束时间注册给EventTime定时器
        ctx.registerEventTimeTimer(window.maxTimestamp)
        // 获取当前状态中的时间戳
        val fireTimestamp = ctx.getPartitionedState(stateDesc)
        // 如果是第一次执行，那么将元素的timestamp进行floor操作，取整后加上传入的实例变量interval,得到下一次触发时间并注册，添加到状态中
        if (fireTimestamp.get() == null) {
          val start = timestamp - (timestamp % interval)
          val nextFireTimestamp = start + interval
          ctx.registerEventTimeTimer(nextFireTimestamp)
          fireTimestamp.add(nextFireTimestamp)
        }
        // 此时继续等待
        TriggerResult.CONTINUE
      }
    }

    // 上下文获取状态中的值，并从定时器中清除这个值
    private def clearTimerForState(context: Trigger.TriggerContext): Unit = {
      val timestamp = context.getPartitionedState(stateDesc).get()
      if (timestamp != null) {
        context.deleteEventTimeTimer(timestamp)
      }
    }

    override def toString: String = s"EarlyTriggeringTrigger(${interval})"
  }

  // 类中的every方法，传入interval，作为参数传入此类的构造器，时间转换为毫秒
  object EarlyTriggeringTrigger {
    def every(interval: Time) = new EarlyTriggeringTrigger(interval.toMilliseconds)
  }