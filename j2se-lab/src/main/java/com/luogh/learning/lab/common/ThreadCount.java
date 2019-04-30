package com.luogh.learning.lab.common;

public class ThreadCount {

  public static void main(String[] args) {
    ThreadGroup curGroup = Thread.currentThread().getThreadGroup();
    ThreadGroup rootGroup = null;
    int totalNum = 0;

    while(curGroup != null) {
      System.out.println("thread group:" + curGroup.getName() + " with total active thread num:" + curGroup.activeCount());
      if (curGroup.getParent() == null) {
        rootGroup = curGroup;
        totalNum = rootGroup.activeCount();
      }
      curGroup = curGroup.getParent();
    }
    Thread[] threads = new Thread[totalNum];
    rootGroup.enumerate(threads, true);

    for (Thread thread : threads) {
      System.out.println("thread: " + thread.toString());
    }

  }
}
