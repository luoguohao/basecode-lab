package com.luogh.learning.lab.common;

public class ClassLoaderChecker {

  public static void main(String[] args) throws Exception {
    ClassLoader.getSystemClassLoader().loadClass(Main.class.getCanonicalName());
  }

}
