package com.luogh.j2se.test;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;

public class MockitoTest {

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
  @Mock
  private Person person;
  /**
   * 参数捕获
   **/
  @Captor
  private ArgumentCaptor<Person> argumentCaptor;
  /**
   * Instance for spying is created by calling constructor explicitly
   **/
  @Spy
  private List spy = new ArrayList(10);
  /**
   * Instance for spying is created by mockito via reflection (only default constructors supported)
   **/
  @Spy
  private List spy2;

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

    assertEquals("first", mockedList.get(0));
    assertEquals("first", mockedList.get(0));
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

    assertEquals("element", mockedList.get(999));
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


  /**
   * 为连续的调用做测试桩(stub) 有时候需要为同一个函数调用的不同的返回值或异常做测试桩
   */
  @Test
  public void testConsecutiveStub() {
    List list = mock(List.class);
    when(list.add(1)).thenThrow(new RuntimeException())
        .thenReturn(true);

    // First call: throw new RuntimeException
    thrown.expect(RuntimeException.class);
    list.add(1);
    // Second call: equals true
    Assert.assertTrue(list.add(1));
    // any consecutive call: prints "foo" as well(last stubbing wins)
    Assert.assertTrue(list.add(1));

    // more simple usage
    // first call return 1 , second call return 2 ,third call return 3 ...
    when(list.get(0)).thenReturn(1, 2, 3);
    assertEquals(1, list.get(0));
    assertEquals(2, list.get(0));
    assertEquals(3, list.get(0));
  }


  /**
   * 为回调做测试桩
   */
  @Test
  public void testCallback() {
    List list = mock(List.class);
    when(list.get(0)).then((Answer<String>) invocation -> {
      Object[] args = invocation.getArguments();
      return "called with arguments:" + args[0];
    });
    assertEquals("called with arguments:0", list.get(0));
  }


  /**
   * doReturn() doThrow() doAnswer() doNothing() doCallRealMethod()方法的运用
   *
   * 通过when(Object)为无返回值的函数打桩有不同的方法,因为编译器不喜欢void函数在括号内… 使用doThrow(Throwable)
   * 替换stubVoid(Object)来为void函数打桩是为了与doAnswer() 等函数族保持一致性。
   *
   * 用于测试void函数
   */
  @Test
  public void testDoMethod() {
    List list = mock(List.class);
    thrown.expect(RuntimeException.class);

    doReturn(false).when(list).add(1);
    doThrow(new RuntimeException()).when(list).add(0);

    assertEquals(false, list.add(1));
    list.add(0);
  }


  /**
   * 监控真实对象 你可以为真实对象创建一个监控(spy)对象。当你使用这个spy对象时真实的对象也会也调用， 除非它的函数被stub了。尽量少使用spy对象，使用时也需要小心形式，例如spy对象可以用
   * 来处理遗留代码。 Mockito并不会为真实对象代理函数调用，实际上他会拷贝真实对象。因此 如果你保留了真实对象，并且与之交互，不要期望从监控对象上得到正确的结果，
   * 如果你在监控对象上调用一个没有被stub的函数，时并不会调用真实对象对应的函数 你不会在真实对象上看到任何效果
   */
  @Test
  public void testSpy() {
    List<String> list = new ArrayList<>();
    list.add("add");
    List<String> spy = Mockito.spy(list);

    assertEquals("add", spy.get(0)); // spy is copy the real object

    // Impossible: real method is called so spy.get(0) throws IndexOutOfBoundsException
    thrown.expect(IndexOutOfBoundsException.class);
    when(spy.get(1)).thenReturn("test");
    // use doReturn() for stubbing instead.
    doReturn("test").when(spy).get(1);
    assertEquals("test", spy.get(1));

    // optionally, you can stub out some methods
    when(spy.size()).thenReturn(100);

    // using the spy calls *real* methods
    spy.add("one");
    spy.add("two");

    assertEquals("add", spy.get(0));
    assertEquals("one", spy.get(1));

    // size() method was stubbed
    assertEquals("add", list.get(0));

    // optionally, you can verify
    verify(spy).add("one");
    verify(spy).add("two");

    assertEquals(1, list.size());
  }


  @Test
  public void testSpyAnnotation() {
    when(spy.size()).thenReturn(10);
    assertEquals(10, spy.size());

    when(spy2.size()).thenReturn(10);
    assertEquals(10, spy2.size());
  }

  /**
   * 修改没有测试桩的调用的默认返回值
   */
  @Test
  public void testDefaultReturnValue() {
    List list = mock(List.class, Mockito.RETURNS_SMART_NULLS);
    // calling unstubbing method
    // list.get(0) doesn't yield NullPointerException this time!
    // Instead, SmartNullPointerException is thrown.
    Object result = list.get(0);
    Assert.assertNotNull(result.toString());
  }


  /**
   * 为下一步的断言捕获参数,验证参数值 可以使用@Captor简化ArguemntCaptor的创建
   */
  @Test
  public void testArgumentCaptor() {
    List mock = mock(List.class);
    mock.add(new Person("John", 12));
//    ArgumentCaptor<Person> argument = ArgumentCaptor.forClass(Person.class);
    // 参数捕获
    verify(mock).add(argumentCaptor.capture());
    assertEquals("John", argumentCaptor.getValue().getName());
  }


  private ArgumentMatcher<String> isValid() {
    return argument -> true;
  }

  @Getter
  @Setter
  @AllArgsConstructor
  static class Person {

    private String name;
    private int age;
  }
}
