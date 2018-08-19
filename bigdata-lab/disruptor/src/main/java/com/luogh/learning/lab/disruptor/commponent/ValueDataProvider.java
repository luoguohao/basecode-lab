package com.luogh.learning.lab.disruptor.commponent;

import com.google.common.base.Preconditions;
import com.lmax.disruptor.DataProvider;
import com.lmax.disruptor.RingBuffer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ValueDataProvider implements DataProvider<ValueEvent> {

  private final RingBuffer<ValueEvent> ringBuffer;

  public ValueDataProvider(RingBuffer<ValueEvent> ringBuffer) {
    Preconditions.checkNotNull(ringBuffer);
    this.ringBuffer = ringBuffer;
  }


  @Override
  public ValueEvent get(long sequence) {
    return ringBuffer.get(sequence);
  }


  public void publish(long data) {
    long sequence = ringBuffer.next();
    ValueEvent event = ringBuffer.get(sequence);
    event.setValue(data);
    ringBuffer.publish(sequence);
    log.info("publish data:{} for the sequence {}.", event, sequence);
  }
}