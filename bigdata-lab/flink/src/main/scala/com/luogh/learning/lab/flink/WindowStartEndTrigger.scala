package com.luogh.learning.lab.flink

import grizzled.slf4j.Logging
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * this trigger will just trigger at the window start & end time
  *
  * @param interval
  * @tparam T
  */
class WindowStartEndTrigger[T <: AnyRef] extends Trigger[T, TimeWindow] with Logging {

  private val triggerTypeStat = new ValueStateDescriptor[TriggerType]("triggerTypeStat", createTypeInformation[TriggerType])


  override def onEventTime(time: Long,
                           window: TimeWindow,
                           ctx: Trigger.TriggerContext): TriggerResult =
    if (time == window.maxTimestamp) {
      logger.debug(s"onEventTime(${time}) ===== fire at window end for window:${window}")
      ctx.getPartitionedState(triggerTypeStat).update(SessionEndTrigger(time))
      TriggerResult.FIRE // window end
    } else {
      TriggerResult.CONTINUE
    }


  override def onProcessingTime(time: Long,
                                window: TimeWindow,
                                ctx: Trigger.TriggerContext): TriggerResult = {
    logger.info(s"onProcessingTime(${time})")
    TriggerResult.CONTINUE
  }


  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    logger.debug(s"clear(${window}) ===== delete window max timer: ${window.maxTimestamp()}")
    ctx.deleteEventTimeTimer(window.maxTimestamp)
    ctx.getPartitionedState(triggerTypeStat).clear() // clear stat
  }

  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {
    // only register a timer if the watermark is not yet past the end of the merged window
    // this is in line with the logic in onElement(). If the watermark is past the end of
    // the window onElement() will fire and setting a timer here would fire the window twice.
    val windowMaxTimestamp = window.maxTimestamp
    if (windowMaxTimestamp > ctx.getCurrentWatermark) {
      logger.debug(s"onMerge(${window}) ===== register a window max timer:${window.maxTimestamp()} " +
        s"if windowMaxTimestamp is larger than the current watermarker ")
      ctx.registerEventTimeTimer(windowMaxTimestamp)
    }
  }

  override def canMerge: Boolean = true


  override def onElement(element: T, timestamp: Long,
                         window: TimeWindow,
                         ctx: Trigger.TriggerContext): TriggerResult = {
    if (window.getStart == timestamp) { // if window start, fire the window immediately to process window operator
      logger.debug(s"onElement(${element}) =====  window started, fire the window:${window} immediately")
      ctx.getPartitionedState(triggerTypeStat).update(SessionStartTrigger(timestamp))
      TriggerResult.FIRE
    } else if (window.maxTimestamp <= ctx.getCurrentWatermark) { // if the watermark is already past the window fire immediately
      logger.debug(s"onElement(${element}) ===== watermark is already past the window end, fire the window:${window} immediately")
      ctx.getPartitionedState(triggerTypeStat).update(SessionEndTrigger(timestamp))
      TriggerResult.FIRE
    } else {
      logger.debug(s"onElement(${element}) ===== register window end [${window.maxTimestamp()}] timer for window:${window}")
      ctx.registerEventTimeTimer(window.maxTimestamp)
      TriggerResult.CONTINUE
    }
  }
}
