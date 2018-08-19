package com.luogh.j2se.test;

import static org.junit.Assert.assertEquals;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import org.junit.Test;

public class MethodHandleTest {


  @Test
  public void testInvokeExact() throws Throwable {
    MethodHandles.Lookup lookup = MethodHandles.lookup();
    // mt is (char,char)String
    MethodType mt = MethodType.methodType(String.class, char.class, char.class);
    MethodHandle mh = lookup.findVirtual(String.class, "replace", mt);
    String s = (String) mh.invokeExact("daddy", 'd', 'n');
    // invokeExact(Ljava/lang/String;CC)Ljava/lang/String;
    assertEquals(s, "nanny");

  }

  @Test
  public void testInvokeWithArguments() throws Throwable {
    MethodHandles.Lookup lookup = MethodHandles.lookup();
    MethodType mt = MethodType.methodType(String.class, char.class, char.class);
    MethodHandle mh = lookup.findVirtual(String.class, "replace", mt);
    // weakly typed invocation (using MHs.invoke)
    String s = (String) mh.invokeWithArguments("sappy", 'p', 'v');
    assertEquals(s, "savvy");
  }


}
