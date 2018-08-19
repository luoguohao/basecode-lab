package com.luogh.learning.lab.disruptor.commponent;

import com.lmax.disruptor.EventHandler;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ValueEventHandler implements EventHandler<ValueEvent> {

  private final static AtomicInteger IDENTIFIER = new AtomicInteger(0);
  private final int id;

  public ValueEventHandler() {
    id = IDENTIFIER.getAndIncrement();
  }

  @Override
  public void onEvent(ValueEvent valueEvent, long sequence, boolean endOfBatch) throws Exception {
    log.info("received new event:{} with sequence:{} and endOfBatch:{} at EventHandler[{}]", valueEvent, sequence,
        endOfBatch, id);
  }
}