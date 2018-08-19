package com.luogh.learning.lab.multithread;

import java.util.ArrayList;
import java.util.List;

/**
 * 自定义实现公平锁，实现避免处于线程饥饿状态（starvation,slipped condition,fairness,missing signal，nested monitor
 * lockout（嵌套管程锁死)问题的解决方案）
 *
 * 出现线程starvation现象有以下几种产生原因： 1）高优先级的线程一直占用cup时间片，而低优先级的线程一直处于阻塞状态，这个时候低优先级的线程就是处于startvation状态
 * 2）在访问synchronized块的时候，线程调度器只是随机的从所有的等待线程中获取一个，进行synchronized块的访问，可能某个等待线程一直不能被赋予访问synchronized块的权利
 * 3）在某个锁对象调用notify方法的时候是从所有在该锁对象上等待的线程中随机获取一个来占有该锁对象。可能某个线程永远无法被唤醒。
 *
 * @author administrator
 */
public class ThreadFairnessLock {

  private boolean isLocked = false;  //当前锁是否已被占用
  private Thread lockingThread = null; //占用当前锁的线程
  private List<QueueObject> waitingThreads = new ArrayList<QueueObject>(); //等待获取当前锁的所有等待线程的队列

  /**
   * 每个线程试图调用lock方法占用锁对象
   */
  public void lock() throws InterruptedException {
    /**
     * 局部变量，每个线程拥有自己的queueObejct，分别存放在各个线程中的线程栈中。
     * 之所以，定义一个局部变量，并且该局部变量为控制该线程的唤醒和等待状态（即是一把锁）的目的是
     * 	如果有多个线程试图占有一个锁，为了防止在某个锁调用notify的时候，某个等待的线程出现starvation（饥饿）即可能永远
     * 无法被唤醒，因为唤醒是随机的唤醒某一个线程。
     * 而这个使用一个局部变量锁，就可以保证每把queueObejct锁只是被一个单一的线程所控制。这样在调用notify的时候，处于等待的
     * 单一的线程一定会被唤醒，而不会出现starvation现象。
     */
    QueueObject queueObject = new QueueObject();//该对象相当于一个监视器或者是semaphore ，只监听一个线程

    boolean isLockedForThisThread = true; //判断当前线程是否占用该锁对象

    synchronized (this) {
      waitingThreads.add(queueObject); //每次试图占用锁，现在等待线程集合中添加一个对象
    }

    /**
     *  加while循环确保等待线程一直处于等待状态，直到isLockedForThisThread = false;防止线程（spurious wakeup）虚假唤醒,
     */
    while (isLockedForThisThread) {

      synchronized (this) {//对于竞争条件isLocked和waitingThreads，使用内部锁定，并且为防止Slipped conditions，
//					就是说， 从一个线程检查某一特定条件到该线程操作此条件期间，这个条件已经被其它线程改变，导致第一个线程在该条件上执行了错误的操作
//					即将检查和设置锁的状态放在同一个同步代码块来防止Slipped Conditions;	

        isLockedForThisThread = isLocked || waitingThreads.get(0) != queueObject;
        /**
         *	1）如果当前锁对象没有被占用并且当前线程为等待线程队列中的第一个，此时isLockedForThisThread为false。说明此时该线程没有占用该锁，并且该等待线程为
         * 		等待线程队列中的第一个，根据公平锁的原则（等待时间最长的线程优先获取锁对象），此时可以给当前线程占用锁对象。
         *  2）如果当前锁对象没有被占用但是当前线程不是等待线程队列的第一个，根据公平锁的原则（等待时间最长的线程优先获取锁对象），应该将等待时间最长的线程即队列中的
         *     第一个线程赋予占有锁对象的权利，而不是当前线程。所以当前线程应该继续等待。
         *  3）如果当前锁对象已经被占用，那么当前等待线程继续存在于等待线程队列中，直到拥有该锁对象的线程调用unlock()方法。
         *
         */
        if (!isLockedForThisThread) {
          //如果当前线程获得该锁对象的权利
          isLocked = true; //设置当前锁对象已被占用
          waitingThreads.remove(queueObject);//从等待线程队列中删除当前线程对应的对象
          lockingThread = Thread.currentThread(); //设置当前占有该锁对象的线程为当前线程
        }
      }

      try {
        /**
         * 将quequeObject.doWait()方法放在synchronized(ThreadFairnessLock)之外是为了避免当前等待线程在获得监视器的锁对象（quequeObject）时处于等待状态，
         * 而使得
         * 		synchronized(this){ this对象指的是ThreadFairnessLock对象。而不是queueObject对象。这样如果queueObject调用doWait方法，那么当前线程处于等待
         * 							//状态，当前线程只会释放queueObject对象的内部锁（synchronized），而不会释放ThreadFairnessLock的内部锁。即这段内部锁代码
         * 							//将一直处于等待状态，而其他线程无法在调用unlock（）方法的时候获取ThreadFairnessLock的内部锁。这样所有线程将无限得等待ThreadFairnessLock的内部锁。
         * 							//这种现象成为nested monitor lockout（嵌套管程锁死）。所以将queueObject.doWait()放在synchronized(this)之外。
         * 			queueObject.doWait();
         *    }
         */
        queueObject.doWait(); //如果不能获取当前锁对象，将该queueObject对象上的线程对应的对象设置为等待状态
      } catch (InterruptedException e) {
        synchronized (this) {
          waitingThreads.remove(queueObject); //如果等待状态被中断，将等待的线程从等待线程队列中移除
        }
      }
    }
  }

  /**
   * 调用unlock方法将释放对当前锁对象的占有的线程
   */
  public synchronized void unlock() {
    if (this.lockingThread != Thread.currentThread()) { // 如果当前调用unlock方法的线程不是占有锁对象的真实线程，报错
      throw new IllegalMonitorStateException(""
          + "Calling thread has not locked this lock");
    }
    isLocked = false;
    lockingThread = null;
    if (waitingThreads.size() > 0) {
      waitingThreads.get(0).doNotify(); //唤醒在当前锁对象所等待的线程队列中的第一个等待时间最长的线程（公平锁的原则）
    }
  }
}


/**
 * 用于唤醒和等待在改QueueObject对象上的各个线程。为一个信号灯（Semaphore）。之所以使用信号灯控制线程而不是单独使用某个对象的内部锁直接控制线程的原因是： 使用某个对象的内部锁
 * this.notify()方法去唤醒等待的线程的操作如果发生在this.wait()方法调用之前，那么当前等待的线程就无法接收到notify方法发送的 唤醒信号即出现missing signal
 * 的现象，从而导致等待的线程可能一直处于等待状态，而不能唤醒。 使用信号灯（Semaphore）的话，通过在内部定义一个竞争条件（isNotified）。来控制当前线程的状态。此时，即使是在doWait()方法前调用doNotify()方法，当前在该
 * 信号灯等待的线程同样可以通过信号灯中的竞争条件（isNotified）的状态来使等待的线程被唤醒。这样就保证了发送的信号不被丢失。
 *
 * @author administrator
 */
class QueueObject {

  private boolean isNotified = false;

  /**
   * 将试图获取当前queueObject对象的线程设置为等待状态。(当前queueObject对象即是一把锁，该锁只被一个线程占有和等待)
   */
  public synchronized void doWait() throws InterruptedException {
    while (!isNotified) {
      this.wait();
    }
    this.isNotified = false;
  }

  /**
   * 唤醒在当前queueObject对象上等待的线程
   */
  public synchronized void doNotify() {
    this.isNotified = true;
    this.notify();
  }

  public boolean equals(Object o) {
    return this == o;
  }
}