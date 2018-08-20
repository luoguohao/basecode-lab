package com.luogh.j2se.test;

import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.MutableCallSite;
import org.junit.Assert;
import org.junit.Test;

/**
 * invokedynamic指令需要与MethodHandle方法句柄结合起来使用。该指令的灵活性在很大程度上取决于方法句柄的灵活性。
 * 对于invokedynamic指令来说，在Java源代码中是没有直接的对应产生方式的。这也是invokedynamic指令的新颖之处。
 * 它是一个完全的Java字节代码规范中的指令。传统的Java编译器并不会帮开发人员生成invokedynamic指令。为了利用
 * invokedynamic指令，需要开发人员自己来生成包含这个指令的Java字节代码。因为这个指令本来就是设计给动态语言的编译
 * 器使用的，所以这种限制也是合理的。对于一般的程序来说，如果希望使用这个指令，就需要使用操作Java字节代码的工具来完成。
 * 在字节代码中每个出现的invokedynamic指令都成为一个动态调用点（dynamic call site）。每个动态调用点在初始化的时候，
 * 都处于未链接的状态。在这个时候，这个动态调用点并没有被指定要调用的实际方法。当Java虚拟机要执行invokedynamic指令时，
 * 首先需要链接其对应的动态调用点。在链接的时候，Java虚拟机会先调用一个启动方法（bootstrap method）。这个启动方法
 * 的返回值是java.lang.invoke.CallSite类的对象。在通过启动方法得到了CallSite之后，通过这个CallSite对象的
 * getTarget方法可以获取到实际要调用的目标方法句柄。有了方法句柄之后，对这个动态调用点的调用，实际上是代理给方法句
 * 柄来完成的。也就是说，对invokedynamic指令的调用实际上就等价于对方法句柄的调用，具体来说是被转换成对方法句柄的
 * invoke方法的调用。
 *
 * Java 7中提供了三种类型的动态调用点CallSite的实现分别是：
 * java.lang.invoke.ConstantCallSite
 * java.lang.invoke.MutableCallSite
 * java.lang.invoke.VolatileCallSite
 * 这些CallSite实现的不同之处在于所对应的目标方法句柄的特性不同。
 * ConstantCallSite所表示的调用点绑定的是一个固定的方法句柄，一旦链接之后，就无法修改；
 * MutableCallSite所表示的调用点则允许在运行时动态修改其目标方法句柄，即可以重新链接到新的方法句柄上；
 * 而VolatileCallSite的作用与MutableCallSite类似，不同的是它适用于多线程情况，
 * 用来保证对于目标方法句柄所做的修改能够被其他线程看到。这也是名称中volatile的含义所在，类似于Java中的volatile关
 * 键词的作用。

 * 虽然CallSite一般同invokedynamic指令结合起来使用，但是在Java代码中也可以通过调用CallSite的dynamicInvoker
 * 方法来获取一个方法句柄。
 * 调用这个方法句柄就相当于执行invokedynamic指令。通过此方法可以预先对CallSite进行测试，以保证字节代码中的
 * invokedynamic指令的行为是正确的，毕竟在生成的字节代码中进行调试是一件很麻烦的事情。
 *
 * https://blog.csdn.net/yushuifirst/article/details/48028859
 */
public class CallSiteTest {

  /**
   * ConstantCallSite要求在创建的时候就指定其链接到的目标方法句柄。每次该调用点被调用的时候，总是会执行对应的目标方法句柄。
   * @throws Throwable
   */
  @Test
  public void testConstantCallSite() throws Throwable {
    MethodHandles.Lookup lookup = MethodHandles.lookup();
    MethodType methodType = MethodType.methodType(String.class, int.class, int.class);
    MethodHandle methodHandle = lookup.findVirtual(String.class, "substring", methodType);
    ConstantCallSite callSite = new ConstantCallSite(methodHandle);
    MethodHandle invoker = callSite.dynamicInvoker();
    String result = (String)invoker.invoke("Hello", 2, 3);
    Assert.assertEquals("l", result);
  }


  /**
   * MutableCallSite则允许对其所关联的目标方法句柄进行修改。修改操作是通过setTarget方法来完成的。
   * 在创建MutableCallSite的时候，既可以指定一个方法类型MethodType，又可以指定一个初始的方法句柄。
   * @throws Throwable
   */
  @Test
  public void testMutableCallSite() throws Throwable {
    MethodType type = MethodType.methodType(int.class, int.class, int.class);
    MutableCallSite callSite = new MutableCallSite(type);
    MethodHandle invoker = callSite.dynamicInvoker();
    MethodHandles.Lookup lookup = MethodHandles.lookup();
    MethodHandle mhMax = lookup.findStatic(Math.class, "max", type);
    MethodHandle mhMin = lookup.findStatic(Math.class, "min", type);
    callSite.setTarget(mhMax);
    int result = (int) invoker.invoke(3, 5); //值为5
    Assert.assertEquals(5, result);
    callSite.setTarget(mhMin);
    result = (int) invoker.invoke(3, 5); //值为3
    Assert.assertEquals(3, result);
  }
}
