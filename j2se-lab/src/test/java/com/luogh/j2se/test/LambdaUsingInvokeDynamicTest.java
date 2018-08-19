package com.luogh.j2se.test;

import java.util.Arrays;

/**
 * 查看lambda是如果使用invokeDynamic指令的
 * 可以通过javap -v -p com.luogh.j2se.test.LambdaUsingInvokeDynamicTest 看到字节码
 */
public class LambdaUsingInvokeDynamicTest {

  public static void main(String[] args) {
    Runnable r = () -> System.out.println(Arrays.toString(args));
    r.run();
  }
}
