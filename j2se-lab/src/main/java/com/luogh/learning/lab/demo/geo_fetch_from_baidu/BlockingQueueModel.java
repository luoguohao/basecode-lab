package com.luogh.learning.lab.demo.geo_fetch_from_baidu;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/** 
 * 一个的生产者-中间人-消费者模型
 *  
 * @author KevinJom 
 */  
public class BlockingQueueModel {  
    public static void main(String[] args) {  
        new BlockingQueueModel().go();  
    }  
  
    private void go() {  
        // 这里简单的说一下BlockingQueue的实现,它基于生产者-消费者模式，其中有两个重要的阻塞方法  
        // put()和take()，而这两个方法的实现用到了Lock和Condition，具体实现请参考API  
        BlockingQueue<String> queue = new ArrayBlockingQueue<String>(10); 
        Producer producer = new Producer(queue, 100, "peak",500);
        Thread t1 = new Thread(producer); // 生产者线程  
        Thread t2 = new Thread(producer); // 第二个生产者线程 
        
        BlockingQueue<String> queueMedia = new ArrayBlockingQueue<String>(200);
        
        Mediator mediator = new Mediator(queue,queueMedia, 10);
        Thread t3 = new Thread(mediator); // 中间人线程 
        Thread t4 = new Thread(mediator); // 中间人线程 
        Thread t5 = new Thread(mediator); // 中间人线程 
        
        Thread t6 = new Thread(new Customer(queueMedia)); // 消费者线程  
        t1.start();  
        t2.start();  
        t3.start();
        t4.start();
        t5.start();
        t6.start();
    }  
  
     private class Producer implements Runnable {  
        private BlockingQueue<String> queue;  
        private int timeout; // 生产一个产品后暂停的时间  
        private String category; // 仅仅起标记产品作用  
        private volatile Integer cnt = 0;
        
        public Producer(BlockingQueue<String> queue, int timeout,String category,Integer cnt) {  
            super();  
            this.queue = queue;  
            this.timeout = timeout;  
            this.category = category;
            this.cnt = cnt;
        }  
  
        public void run() {
        	boolean over = false;
            while (!Thread.currentThread().isInterrupted()&&!over) {  
                try { 
                	synchronized (cnt) {
                		cnt--;
                		if(cnt<=0){
                			over = true;
                			System.out.println(Thread.currentThread().getName()+" product over!!!");
                			continue;
                		}
					}
                    // put()方法也是一个会阻塞的方法，如果队列已满的时候这个方法会一起阻塞直到  
                    // 队列中重新出现空间为止
                	 System.out.println(Thread.currentThread().getName()+" product put: " + category+"-"+cnt);  
                    queue.put(category+"-"+cnt);  
                } catch (InterruptedException e1) {  
                    e1.printStackTrace();  
                }  
                try {  
                    TimeUnit.MILLISECONDS.sleep(timeout); // 每生产一个产品就暂停timeout毫秒  
                } catch (InterruptedException e) {  
                    e.printStackTrace();  
                }  
            }  
        }  
    }  
  
     private class Mediator implements Runnable {  
         private BlockingQueue<String> queue;  
         private int timeout; // 生产一个产品后暂停的时间  
         private BlockingQueue<String> consumeQueue; 
         private Object object = new Object();
         
         public Mediator(BlockingQueue<String> consumeQueue,BlockingQueue<String> queue, int timeout) {  
             super();  
             this.queue = queue;  
             this.consumeQueue = consumeQueue;  
             this.timeout = timeout;  
         }  
   
         public void run() {
        	 while (!Thread.currentThread().isInterrupted()) {  
                 try {  
                     // put()方法也是一个会阻塞的方法，如果队列已满的时候这个方法会一起阻塞直到  
                     // 队列中重新出现空间为止
                	 synchronized (object) {
                		 String date = consumeQueue.take();
                		 System.out.println("Mediator thread start");
                     	 System.out.println(" Mediator comsume: " + date); 
                     	 queue.put("mediator-"+date);  
                     	 System.out.println(" Mediator reproduct: " + date); 
					}
                	 
                 } catch (InterruptedException e1) {  
                     e1.printStackTrace();  
                 }  
                 try {  
                     TimeUnit.MILLISECONDS.sleep(timeout); // 每生产一个产品就暂停timeout毫秒  
                 } catch (InterruptedException e) {  
                     e.printStackTrace();  
                 }  
             }  
   
         }  
   
     }  
     
    private class Customer implements Runnable {  
        private BlockingQueue<String> queue;  
  
        public Customer(BlockingQueue<String> queue) {  
            super();  
            this.queue = queue;  
        }  
  
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {  
                try {  
                    System.out.println(Thread.currentThread().getName()+" get Medianor product:" + queue.take());  
                } catch (InterruptedException e1) {  
                    e1.printStackTrace();  
                }  
                try {  
                    // 暂停10毫秒，这里主要是为了证明take()是一个阻塞方法，如果 BlockingQueue中  
                    // 没有元素，它会一起阻塞直到队列中有元素为止  
                    TimeUnit.MILLISECONDS.sleep(10);  
                } catch (InterruptedException e) {  
                    e.printStackTrace();  
                }  
            }  
  
        }  
  
    }  
  
}  