package com.luogh.learning.lab.simple;

import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class LambdaApp {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.fromElements(1, 2, 3, 4)
//        .map(x -> new Apple("t", "c"))
//        .filter(Apple::nonNull) // 运行正常
        .filter(Objects::nonNull) // 运行异常
        .print();

    env.execute("test");
  }


  @Getter
  @Setter
  public static class Apple {

    String f;
    String a;

    public Apple() {
    }

    public Apple(String f, String a) {
      this.f = f;
      this.a = a;
    }

    public static boolean nonNull(Apple a) {
      return Objects.nonNull(a);
    }
  }

}
