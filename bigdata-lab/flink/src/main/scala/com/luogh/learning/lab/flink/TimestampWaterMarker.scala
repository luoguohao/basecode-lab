package com.luogh.learning.lab.flink

import grizzled.slf4j.Logging
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
  *
  * @param maxOutOfOrderness The (fixed) interval between the maximum seen timestamp seen in the records,
  *                          and that of the watermark to be emitted.
  * @param maxAllowedTimeExceeded the max allowed watermarker exceed the current event timestamp, this
  *                               parameter is used to trigger session window periodically, but also
  *                               control the watermarker not increment so fast.
  * @param incrementalInterval
  */
class TimestampWaterMarker(maxOutOfOrderness: Long, maxAllowedTimeExceeded: Long, incrementalInterval: Long) extends AssignerWithPeriodicWatermarks[User] with Logging {

  /**
    * The timestamp of the last emitted watermark.
    */
  private var lastEmittedWatermark = Long.MinValue

  /**
    * The current maximum timestamp seen so far.
    */
  private var currentMaxTimestamp = Long.MinValue + maxOutOfOrderness

  /**
    * The last time to invoke the getCurrentWatermark() method to get the watermarker
    */
  private var lastWatermarkInvokeTime = Long.MinValue

  override def getCurrentWatermark: Watermark = {
    val potentialWM = currentMaxTimestamp - maxOutOfOrderness
    if (potentialWM == lastEmittedWatermark) {
      // if last emitted watermarker is still the same as before, so we believe that they may be
      // no element emit, so in order to make sure that session window will be triggered periodically,
      // we trying to increment the watermarker, once that the watermarkerInvoke time is much greater
      // than the last EmmittedWatermarker at the session window gap granularity, new watermarker should
      // be emitted
      lastWatermarkInvokeTime += incrementalInterval
      val interval = lastWatermarkInvokeTime - lastEmittedWatermark
      if (interval > maxAllowedTimeExceeded) {
        lastEmittedWatermark = lastWatermarkInvokeTime
        logger.info(s"getCurrentWatermark() ===== no new element emitted after ${interval}, that is " +
          s"already greater than maxAllowedTimeExceeded : ${maxAllowedTimeExceeded}, so emit new watermarker at the " +
          s"time:${lastEmittedWatermark}")
      }
    } else if (potentialWM > lastEmittedWatermark) {
      // this guarantees that the watermark never goes backwards.
      lastEmittedWatermark = potentialWM
      lastWatermarkInvokeTime = potentialWM
    }
    logger.info(s"emit watermarker: ${lastEmittedWatermark}")
    //    logger.debug(s"call getCurrentWatermark======currentMaxTimestamp:${currentMaxTimestamp}, potentialWM:${potentialWM}, lastEmittedWatermark:${lastEmittedWatermark}")
    new Watermark(lastEmittedWatermark)
  }


  override def extractTimestamp(element: User, previousElementTimestamp: Long): Long = {
    val timestamp = element.timestamp
    if (timestamp > currentMaxTimestamp) {
      currentMaxTimestamp = timestamp
    }
    //    logger.debug(s"call extractTimestamp======timestamp:${timestamp}, " +
    //      s"currentMaxTimestamp:${currentMaxTimestamp}, lastEmittedWatermark:${lastEmittedWatermark}," +
    //      s"previousElementTimestamp:${previousElementTimestamp}")
    timestamp
  }
}