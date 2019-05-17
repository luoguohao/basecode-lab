package com.luogh.learning.lab.flink;

import java.util.Arrays;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.Program;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WindowWordCountProgram implements Program {

  @Override
  public Plan getPlan(String... args) {
    final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

    DataSet<WordCount> text = env.readTextFile(
        "/Users/luogh/Code_Repository/luogh_repo/java_repo/basecode-lab/bigdata-lab/disruptor/logs/disruptor.log")
        .flatMap((String data, Collector<WordCount> collector) ->
            Arrays.stream(data.split(" "))
                .forEach(token -> collector.collect(new WordCount(token, 1)))
        )
        .returns(Types.POJO(WordCount.class))
        .groupBy("word")
        .reduce((key1, key2) -> new WordCount(key1.getWord(), key1.getCount() + key2.getCount()));


    try {
      text.print();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return env.createProgramPlan();
  }
}
