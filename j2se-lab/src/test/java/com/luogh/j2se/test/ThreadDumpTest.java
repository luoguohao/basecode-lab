package com.luogh.j2se.test;

import org.junit.Test;

public class ThreadDumpTest {

  static final Object monitor = new Object();
  static final Object monitor2 = new Object();

  @Test
  public void testMonitorThreadDump() throws Exception {
    new Thread(() -> new LockRequrier().acquireLock()).start();
    Thread.sleep(100);
    synchronized (monitor) {
    }
  }

  @Test
  public void testMonitorWaitingThreadDump() throws Exception {
    Thread.sleep(100);
    synchronized (monitor) {
      monitor.wait();
    }
  }

  @Test
  public void testDeadLockThreadDump() throws Exception {
    new Thread(() -> {
      try {
        new LockRequrier1().acquireLock();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }).start();
    new Thread(() -> {
      try {
        new LockRequrier2().acquireLock();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }).start();
    Thread.currentThread().join();
  }


  private final class LockRequrier {

    public void acquireLock() {
      synchronized (ThreadDumpTest.monitor) {
        while(true) {

        }
      }
    }
  }

  private final class LockRequrier1 {
    public void acquireLock() throws Exception {
      synchronized (ThreadDumpTest.monitor) {
        Thread.sleep(500);
        synchronized (ThreadDumpTest.monitor2) {
          while (true) {

          }
        }
      }
    }
  }


  private final class LockRequrier2 {
    public void acquireLock() throws Exception {
      synchronized (ThreadDumpTest.monitor2) {
        Thread.sleep(500);
        synchronized (ThreadDumpTest.monitor) {
          while(true) {}
        }
      }
    }
  }
}
