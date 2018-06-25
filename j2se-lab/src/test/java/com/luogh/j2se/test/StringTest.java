package com.luogh.j2se.test;

import org.junit.Assert;
import org.junit.Test;

/**
 * 验证字符串常量池的==比较
 */
public class StringTest {

  private static final String sA = "test";
  private static final String sB = "test2";
  private static final String sC = sA + sB;

  /**
   * 静态字符串常量在常量池中
   * 相同的字符串只有一个
   */
  @Test
  public void testStaticString() {
    Assert.assertSame(sC, "testtest2");
  }


  /**
   * 非静态变量运行时创建，如果常量池中不存在，那么会将
   * 新的字符串增加到字符串常量池中，但在未加入之前，指向
   * 的地址和常量池中的不一样
   */
  @Test
  public void testStringConstant() {
    String a = "test";
    String b = "test2";
    String c = a + b;
    String d = "testtest2";
    Assert.assertNotSame(d, c);
  }


  /**
   * 非静态变量运行时创建，如果常量池中不存在，那么会将
   * 新的字符串增加到字符串常量池中，通过使用intern()
   * 方法可以获取到常量池中的字符串进行==比较
   */
  @Test
  public void testStringInternal() {
    String a = "test";
    String b = "test2";
    String c = a + b;
    String d = "testtest2";
    Assert.assertSame(d, c.intern());
  }
}
