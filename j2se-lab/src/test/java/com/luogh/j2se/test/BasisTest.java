package com.luogh.j2se.test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import org.junit.Assert;
import org.junit.Test;

public class BasisTest {

  @Test
  public void testInet() throws Exception {
    InetAddress inetAddress = InetAddress.getByName("sz-pg-smce-devhadoop-007.tendcloud.com");
    InetSocketAddress add = new InetSocketAddress("172.23.4.246", 11);
    InetSocketAddress isa = new InetSocketAddress(add.getHostName(), add.getPort());
    System.out.println(add.getHostName());
    System.out.println(add.toString());
    System.out.println(isa.getHostName());
    System.out.println(isa.toString());
    System.out.println(inetAddress.getHostName());
    System.out.println(inetAddress.getHostAddress());
    String localhost = InetAddress.getLocalHost().getCanonicalHostName();
    System.out.println(localhost);
    String hostAddress = InetAddress.getLocalHost().getHostName();
    System.out.println(hostAddress);
  }

  @Test
  public void testArrayQueue() throws Exception {
    ArrayDeque<Integer> queue = new ArrayDeque<>(10);
    queue.add(10);
  }

  @Test
  public void testIsPowerOf2() {
    Assert.assertTrue(isPowerOf2(4));
  }

  @Test
  public void testMod() {
    int res = 6 % 4;
    int res2 = 6 & (4 - 1);
    Assert.assertEquals(res, res2);
  }

  private boolean isPowerOf2(int num) {
    return (num & (num - 1)) == 0;
  }
}
