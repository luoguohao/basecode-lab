package com.luogh.learning.lab.common;

import java.math.BigInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Main {

  private volatile int index = 0;
  private Lock lock = new ReentrantLock();

  public void sync() {
    synchronized (this) {
      index++;
    }
  }

  public void sync2() {
    synchronized (this) {
      System.out.println("test");
    }
  }

  public synchronized void sync3() {
    index++;
  }

  public void sync4() {
    synchronized (this) {
      synchronized (Main.class) {
        System.out.println("test");
      }
    }
  }

  public static void main(String[] args) {
    // Spawn a background thread to compute an enormous number.
    new Thread(){ @Override public void run() {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException ex) {
      }
      System.out.println("start");
      BigInteger.valueOf(999999999).pow(999999999);
      System.out.println("##############################################################");
      System.exit(0);
    }}.start();

// Loop, allocating memory and periodically logging progress, so illustrate GC pause times.
    byte[] b;
    for (int outer = 0; ; outer++) {
      long startMs = System.currentTimeMillis();
      for (int inner = 0; inner < 100000; inner++) {
        b = new byte[1000];
      }
      if (System.currentTimeMillis() - startMs > 100) {
        System.out.println("Iteration " + outer + " took " + (System.currentTimeMillis() - startMs) + " ms");
      }

    }
  }
}
