package com.luogh.learning.lab.common;

import java.util.Objects;

public class LambdaView {

  public LambdaView() {
    MapFunction<Object, Boolean> func = x -> {
      System.out.println("t");
      return x.equals("s");
    };
    java.util.function.Function<Object, Boolean> func1 = Objects::isNull;
  }
}
