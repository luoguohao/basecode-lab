package com.luogh.learning.lab.disruptor;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LongEventMain {


  public static void main(String[] args) throws Exception {
    // Executor that will be used to construct new threads for consumers
    Executor executor = Executors.newCachedThreadPool();
    // Specify the size of the ring buffer, must be power of 2.
    int bufferSize = 1024;
    // Construct the Disruptor
    Disruptor<LongEvent> disruptor = new Disruptor<>(LongEvent::new, bufferSize, executor);
    // Connect the handler
    disruptor
        .handleEventsWith((event, sequence, endOfBatch) -> System.out.println("received Event: " + event));
    // Start the Disruptor, starts all threads running
    disruptor.start();
    // Get the ring buffer from the Disruptor to be used for publishing.
    RingBuffer<LongEvent> ringBuffer = disruptor.getRingBuffer();
    LongEventProducerWithTranslator translator = new LongEventProducerWithTranslator(ringBuffer);
    ByteBuffer bb = ByteBuffer.allocate(8);
    for (long l = 0; true; l++) {
      bb.putLong(0, l);
      translator.onData(bb);
      Thread.sleep(1000);
    }
  }


  @Data
  public static class LongEvent {

    private long value;
  }


  @AllArgsConstructor
  public static class LongEventProducerWithTranslator {

    private final RingBuffer<LongEvent> ringBuffer;

    private static final EventTranslatorOneArg<LongEvent, ByteBuffer> TRANSLATOR =
        (event, sequence, bb) -> event.setValue(bb.getLong(0));

    public void onData(ByteBuffer bb) {
      ringBuffer.publishEvent(TRANSLATOR, bb);
    }
  }
}
