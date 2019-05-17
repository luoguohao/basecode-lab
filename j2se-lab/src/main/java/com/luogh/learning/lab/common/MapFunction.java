package com.luogh.learning.lab.common;

public interface MapFunction<T, R> extends Function {

  R apply(T value);
}