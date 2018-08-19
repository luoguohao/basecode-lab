package com.luogh.learning.lab.disruptor;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.luogh.learning.lab.disruptor.commponent.ValueEvent;
import com.luogh.learning.lab.disruptor.commponent.ValueEventHandler;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DisruptorExchangerWithDSL {

  private final static ThreadFactory THREAD_FACTORY = new ThreadFactoryBuilder()
      .setNameFormat("disruptor-%d").setDaemon(false).build();

  private RingBuffer<ValueEvent> ringBuffer;
  private Disruptor<ValueEvent> disruptor;


  private void setup(Consumer<Disruptor<ValueEvent>> handlerSetter) {
    this.disruptor = new Disruptor<>(ValueEvent.EVENT_FACTORY, 512,
        THREAD_FACTORY);
    handlerSetter.accept(this.disruptor);
    this.ringBuffer = disruptor.start();
    log.info("init disruptor ...");
  }

  private static class ParallelHandler implements Consumer<Disruptor<ValueEvent>> {

    private List<EventHandler<ValueEvent>> handlerList;

    @Override
    public void accept(Disruptor<ValueEvent> disruptor) {
      handlerList = Lists.newArrayList();
      handlerList.add(new ValueEventHandler());
      handlerList.add(new ValueEventHandler());
      handlerList.add(new ValueEventHandler());
      disruptor.handleEventsWith(
          handlerList.toArray(new ValueEventHandler[]{})); // process events in parallel
    }
  }


  private static class DependencyHandler implements Consumer<Disruptor<ValueEvent>> {

    private List<EventHandler<ValueEvent>> handlerList;

    @Override
    public void accept(Disruptor<ValueEvent> disruptor) {
      handlerList = Lists.newArrayList();
      handlerList.add(new ValueEventHandler());
      handlerList.add(new ValueEventHandler());
      handlerList.add(new ValueEventHandler());
      disruptor.handleEventsWith(handlerList.get(0))
          .then(handlerList.get(1), handlerList
              .get(2)); // process with handler 0 first, then handler 1,2 process parallel
    }
  }

  private static class MultiChainHandler implements Consumer<Disruptor<ValueEvent>> {

    private List<EventHandler<ValueEvent>> handlerList;

    @Override
    public void accept(Disruptor<ValueEvent> disruptor) {
      handlerList = Lists.newArrayList();
      handlerList.add(new ValueEventHandler());
      handlerList.add(new ValueEventHandler());
      handlerList.add(new ValueEventHandler());
      handlerList.add(new ValueEventHandler());
      handlerList.add(new ValueEventHandler());
      handlerList.add(new ValueEventHandler());

      disruptor.handleEventsWith(handlerList.get(0))
          .then(handlerList.get(1), handlerList
              .get(2)); // process with handler 0 first, then handler 1,2 process parallel
      disruptor.handleEventsWith(handlerList.get(3))
          .then(handlerList.get(4), handlerList
              .get(5)); // process with handler 3 first, then handler 4,5 process parallel
    }
  }


  public static void main(String[] args) {
    DisruptorExchangerWithDSL exchangerWithDSL = new DisruptorExchangerWithDSL();
    exchangerWithDSL.setup(new MultiChainHandler());
    CompletableFuture.runAsync(new EventValueTranslator(exchangerWithDSL.disruptor));
  }


  @Data
  private static class EventValueTranslator implements EventTranslator<ValueEvent>, Runnable {

    private final Disruptor<ValueEvent> disruptor;
    private long value;

    @Override
    public void translateTo(ValueEvent event, long sequence) {
      event.setValue(value);
      log.info("translateTo event:{} with sequence:{}.", event, sequence);
    }

    @Override
    public void run() {
      while (true) {
        value++;
        disruptor.publishEvent(this);
        try {
          Thread.sleep((long) Math.random() * 1000);
        } catch (InterruptedException e) {
          log.error("failed", e);
        }
      }
    }
  }
}
