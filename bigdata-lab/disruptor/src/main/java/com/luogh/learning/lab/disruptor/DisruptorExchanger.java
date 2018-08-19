package com.luogh.learning.lab.disruptor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import com.luogh.learning.lab.disruptor.commponent.ValueDataProvider;
import com.luogh.learning.lab.disruptor.commponent.ValueEvent;
import com.luogh.learning.lab.disruptor.commponent.ValueEventHandler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.LongStream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DisruptorExchanger {

  private final static ThreadFactory THREAD_FACTORY = new ThreadFactoryBuilder()
      .setNameFormat("event-disruptor-%d").setDaemon(false).build();
  private final static ExecutorService EXECUTOR_SERVICE = Executors
      .newFixedThreadPool(10, THREAD_FACTORY);

  private RingBuffer<ValueEvent> ringBuffer;
  private SequenceBarrier barrier;
  private ValueDataProvider valueDataProvider;
  private ValueEventHandler eventHandler;


  public void setup() {
    this.ringBuffer = RingBuffer
        .create(ProducerType.SINGLE, ValueEvent.EVENT_FACTORY, 1024, new SleepingWaitStrategy());
    this.valueDataProvider = new ValueDataProvider(this.ringBuffer);
    this.eventHandler = new ValueEventHandler();
    this.barrier = ringBuffer.newBarrier();

    BatchEventProcessor<ValueEvent> eventProcessor = new BatchEventProcessor<>(
        valueDataProvider, barrier, eventHandler);
    ringBuffer.addGatingSequences(eventProcessor.getSequence());
    // Each EventProcessor can run on a separate thread
    EXECUTOR_SERVICE.submit(eventProcessor);

    log.info("init disruptor ...");
  }


  public static void main(String[] args) {
    DisruptorExchanger disruptor = new DisruptorExchanger();
    disruptor.setup();
    LongStream.range(0, 10000).parallel().forEach(disruptor.valueDataProvider::publish);
  }

}
