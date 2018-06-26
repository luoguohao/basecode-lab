package com.luogh.j2se.test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

/**
 * 验证java中的StrongReference、WeakReference、SoftReference、PhantomReference
 */
public class ReferenceTest {


  @Test
  public void testStrongReference() {
    Object reference = new Object();
    Object strongReference = reference; // 通过赋值，获得强引用
    assertSame(reference, strongReference);
    reference = null;
    System.gc(); // 强制gc
    assertNotNull(strongReference); // 强引用不会在gc中被回收
  }


  @Test
  public void testWeakReference() throws Exception {
    ReferenceQueue<String> referenceQueue = new ReferenceQueue<>();
    String reference = new String("test");
    WeakReference<String> weakReference = new WeakReference<>(reference, referenceQueue);

    System.out.println("going to collect " + reference.toString() + "with hashcode " + reference.hashCode());

    new Thread(() -> {
      while(isRun.get()) {
        Object obj = referenceQueue.poll();
        if (obj != null) {
          try {
            Field ref = Reference.class.getDeclaredField("referent");
            ref.setAccessible(true);
            Object result = ref.get(obj);
            assertNull(result);  // weakReference always null
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
      System.out.println("exit");
    }).start();

    assertSame(reference, weakReference.get());
    reference = null;
    System.gc();

    assertNull(weakReference.get()); // 一旦没有指向referent的强引用, weak reference在GC后会被自动回收
    Thread.sleep(1000);
    isRun.set(false);
  }


  @Test
  public void testWeakHashMapReference() throws Exception {
    Object key = new Object();
    Object value = new Object();
    WeakHashMap<Object, Object> weakHashMap = new WeakHashMap<>();
    weakHashMap.put(key, value);
    assertTrue(weakHashMap.containsValue(value));
    key = null;
    System.gc();
    Thread.sleep(1000); // 等待无效 entries 进入 ReferenceQueue 以便下一次调用 getTable 时被清理
    assertFalse(weakHashMap.containsValue(value)); // 一旦没有指向 key 的强引用, WeakHashMap 在 GC 后将自动删除相关的 entry
  }


  /**
   * SoftReference于WeakReference的特性基本一致,最大的区别在于SoftReference会尽可能长的保留引用直到
   * JVM 内存不足时才会被回收(虚拟机保证),这一特性使得SoftReference非常适合缓存应用
   * SoftReference比WeakReference生命力更强，当JVM的内存不吃紧时，即使引用的对象被置为空了，Soft还可以
   * 保留对该对象的引用，此时的JVM内存池实际上还保有原来对象，只有当内存吃紧的情况下JVM才会清除Soft的引用
   * 对象，并且会在未来重新加载该引用的对象。而WeakReference则当清理内存池时会自动清理掉引用的对象。
   */
  @Test
  public void testSoftReference() throws Exception {
    ReferenceQueue<String> referenceQueue = new ReferenceQueue<>();
    String reference = new String("test");
    SoftReference<String> softReference = new SoftReference<>(reference, referenceQueue);

    System.out.println("going to collect " + reference.toString() + "with hashcode " + reference.hashCode());

    new Thread(() -> {
      while(isRun.get()) {
        Object obj = referenceQueue.poll();
        if (obj != null) {
          try {
            Field ref = Reference.class.getDeclaredField("referent");
            ref.setAccessible(true);
            Object result = ref.get(obj);
            System.out.println("trying to collect " + result.toString() + "with hashcode " + result.hashCode());
          } catch (Exception e) {
            e.printStackTrace();
          }
        } else {
          System.out.println("..");
        }
      }
      System.out.println("exit");
    }).start();

    reference = null;
    assertNotNull(softReference.get());
    System.gc();

    Thread.sleep(1000);
    assertNotNull(softReference.get()); //  SoftReference只有在JVM out of memory之前才会被回收
    isRun.set(false);
  }


  private AtomicBoolean isRun = new AtomicBoolean(true);

  @Test
  public void testPhantomReference() throws Exception {
    ReferenceQueue<String> referenceQueue = new ReferenceQueue<>();
    String reference = new String("test");
    PhantomReference<String> phantomReference = new PhantomReference<>(reference, referenceQueue);

    System.out.println("going to collect " + reference.toString() + "with hashcode " + reference.hashCode());

    new Thread(() -> {
      while(isRun.get()) {
        Object obj = referenceQueue.poll();
        if (obj != null) {
          try {
            Field ref = Reference.class.getDeclaredField("referent");
            ref.setAccessible(true);
            Object result = ref.get(obj);
            System.out.println("trying to collect " + result.toString() + "with hashcode " + result.hashCode());
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
      System.out.println("exit");
    }).start();

    reference = null;
    assertNull(phantomReference.get()); // always null
    System.gc();

    Thread.sleep(1000);
    assertNull(phantomReference.get());
    Thread.currentThread().sleep(1000);
    isRun.set(false);
  }
}
