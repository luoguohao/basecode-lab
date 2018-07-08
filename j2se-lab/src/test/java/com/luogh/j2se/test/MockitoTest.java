package com.luogh.j2se.test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.util.LinkedList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

public class MockitoTest {

  @Mock
  private Person person;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  /**
   * 用于初始化当前测试类中的使用@mock的对象 方案一：在测试类上：增加 @RunWith(MockitoJUnitRunner.Silent.class) 替换
   * 方案二：在测试类中：增加Rule
   *
   * @Rule public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.LENIENT);
   * 方案三：在执行测试类之前：MockitoAnnotations.initMocks(this);
   */
  @Rule
  public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.LENIENT);


  @Test
  public void testSimpleMock() {
    List mockedList = mock(List.class);
    mockedList.add("one");
    mockedList.add("one");
    mockedList.clear();

    // verification
    verify(mockedList).clear();
    verify(mockedList, times(2)).add("one");
  }


  /**
   * 打桩
   */
  @Test
  public void testStubMock() {
    LinkedList mockedList = mock(LinkedList.class);

    // stubbing
    when(mockedList.get(0)).thenReturn("first");
    when(mockedList.get(1)).thenThrow(new RuntimeException());

    Assert.assertEquals("first", mockedList.get(0));
    Assert.assertEquals("first", mockedList.get(0));
    thrown.expect(RuntimeException.class);
    // thrown.expectMessage("expect exception with error containing the following message");
    mockedList.get(1);  // will throw RuntimeException

    // get(999) not stubbed, returns null
    Assert.assertNull(mockedList.get(999));

    //Although it is possible to verify a stubbed invocation, usually it's just redundant
    //If your code cares what get(0) returns then something else breaks (often before even
    // verify() gets executed). If your code doesn't care what get(0) returns then it should
    // not be stubbed. Not convinced? See here.
    // 验证get(0)被调用的次数
    // 如果当前方法已经被stubbing,那么verify将对他不起作用，所以，如果不是在意方法返回的结果，那么
    // 不使用stubbing
    verify(mockedList).get(0); // nothing happens
  }


  /**
   * 参数匹配器 (matchers)
   */
  @Test
  public void testArgumentMatchers() {
    LinkedList mockedList = mock(LinkedList.class);
    // stubbing using built-in anyInt() argument matcher
    when(mockedList.get(anyInt())).thenReturn("element");
    // stubbing using custom matcher( isValid() returns your own matcher implements)
    when(mockedList.contains(argThat(isValid()))).thenReturn(true);
    // stubbing using multi argument matchers
    when(mockedList.addAll(anyInt(), any())).thenReturn(true);

    // test failed, argument matcher不能和真实的参数值混用
    thrown.expectMessage("Invalid use of argument matchers!");
    when(mockedList.addAll(anyInt(), Lists.newArrayList())).thenReturn(true);

    Assert.assertEquals("element", mockedList.get(999));
    Assert.assertTrue(mockedList.addAll(1, mockedList));
    Assert.assertTrue(mockedList.contains("tst"));

    // verify using an argument matcher
    verify(mockedList).get(anyInt());
  }


  /**
   * 验证函数的确切、最少、从未调用次数
   */
  @Test
  public void testVerify() {
    LinkedList mockedList = mock(LinkedList.class);

    // using mock
    mockedList.add("once");

    mockedList.add("twice");
    mockedList.add("twice");

    mockedList.add("three times");
    mockedList.add("three times");
    mockedList.add("three times");

    // verification: the invoke times of the add() method. default is times(1)
    verify(mockedList).add("once");
    verify(mockedList, times(1)).add("once");
    verify(mockedList, times(2)).add("twice");
    verify(mockedList, times(3)).add("three times");

    // verification: using never() which is an alias to times(0)
    verify(mockedList, never()).add("never happen");

    // verification: using atLeast() or atMost()
    verify(mockedList, atLeast(2)).add("twice");
    verify(mockedList, atMost(3)).add("three times");
  }


  /**
   * 为返回值为void的函数通过Stub抛出异常
   */
  @Test
  public void testStubbing() {
    LinkedList mockedList = mock(LinkedList.class);
    thrown.expect(RuntimeException.class);
    doThrow(new RuntimeException()).when(mockedList).clear();
    mockedList.clear(); // will throw RuntimeException
  }


  /**
   * 验证执行执行顺序
   */
  @Test
  public void testVerifyOrder() {
    // single mock whose methods must be invoked in a particular order
    List singleMock = mock(List.class);

    // using a single mock
    singleMock.add("added first");
    singleMock.add("added second");

    // create InOrder verifier for a single mock
    InOrder inOrder = Mockito.inOrder(singleMock);

    //following will make sure that add is first called with "added first",
    // then with "added second"
    inOrder.verify(singleMock).add("added first");
    inOrder.verify(singleMock).add("added second");

    /////////////////////////////////////////////////
    List firstMock = mock(List.class);
    List secondMock = mock(List.class);

    secondMock.add("add first");
    firstMock.add("add second");
    // create InOrder verifier for a multiple mocks
    InOrder mutipleOrder = Mockito.inOrder(firstMock, secondMock);
    mutipleOrder.verify(secondMock).add("add first");
    mutipleOrder.verify(firstMock).add("add second");
  }


  /**
   * 确保交互(interaction)操作不会执行在mock对象上
   */
  @Test
  public void testVerifyInteraction() {
    List list = mock(List.class);
    List list2 = mock(List.class);

    list.add(1);
    verify(list).add(1); // ordinary verification

    // verify method was never called on a mock
    verify(list, never()).add(2);

    // verify that other mocks were not interacted
    verifyZeroInteractions(list, list2);
  }


  /**
   * 查找冗余的调用
   */
  @Test
  public void testVerificationNoMoreInteractions() {
    List list = mock(List.class);
    list.add("one");
    list.add("two");

    verify(list).add("one");

    // check any of given mocks has any unverified interaction.
    // verify failed, cause list.add("two") not verified.
    thrown.expectMessage("No interactions wanted here");
    verifyNoMoreInteractions(list);
  }

  /**
   * 使用注解方式简化mock对象的创建 注意： 方法一：在运行测试类之前，调用：MockitoAnnotations.initMocks(testClass); 或者使用内置runner:
   * 方法二：使用MockitoJUnitRunner 方法三: 使用rule : MockitoRule
   */
  @Test
  public void testUsingAnnotation() {
    person.setAge(10);
    person.setName("test");
    verify(person).setAge(10);
  }


  private ArgumentMatcher<String> isValid() {
    return argument -> true;
  }

  @Getter
  @Setter
  static class Person {

    private String name;
    private int age;
  }
}
