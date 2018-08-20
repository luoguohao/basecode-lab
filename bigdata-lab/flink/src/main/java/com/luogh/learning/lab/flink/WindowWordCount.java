package com.luogh.learning.lab.flink;

import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

@Slf4j
public class WindowWordCount {

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

    DataStream<WordCount> text = env.readTextFile(
        "/Users/luogh/Code_Repository/luogh_repo/java_repo/basecode-lab/bigdata-lab/disruptor/logs/disruptor.log")
        .flatMap((String data, Collector<WordCount> collector) ->
            Arrays.stream(data.split(" "))
                .forEach(token -> collector.collect(new WordCount(token, 1)))
        )
        .returns(Types.POJO(WordCount.class))
        .keyBy("word")
        .timeWindow(Time.minutes(1), Time.seconds(20))
        .reduce((key1, key2) -> new WordCount(key1.getWord(), key1.getCount() + key2.getCount()));

    text.print().setParallelism(1);
    env.execute("Window WordCount");
  }

}
