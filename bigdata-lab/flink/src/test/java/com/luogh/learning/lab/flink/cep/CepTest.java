//package com.luogh.learning.lab.flink.cep;
//
//import static com.luogh.learning.lab.flink.cep.NFATestUtilities.feedNFA;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.stream.Collectors;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.cep.nfa.NFA;
//import org.apache.flink.cep.nfa.compiler.NFACompiler;
//import org.apache.flink.cep.pattern.Pattern;
//import org.apache.flink.cep.pattern.conditions.SimpleCondition;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
//import org.junit.Test;
//
//public class CepTest {
//
//  @Test
//  public void testCep() throws Exception {
//    final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
//    //构建链接patterns
//    Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
//      private static final long serialVersionUID = 5726188262756267490L;
//
//      @Override
//      public boolean filter(Event value) throws Exception {
//        return value.getName().equals("c");
//      }
//    }).followedBy("middle").where(new SimpleCondition<Event>() {
//      private static final long serialVersionUID = 5726188262756267490L;
//
//      @Override
//      public boolean filter(Event value) throws Exception {
//        return value.getName().equals("a");
//      }
//    }).optional();
//    //创建nfa
//    NFA<Event> nfa = NFACompiler
//        .compile(pattern, Types.POJO(Event.class).createSerializer(env.getConfig()), false);
//  }
//
//
//  @Test
//  public void testCep2() throws Exception {
//    final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
//
//    List<StreamRecord<Event>> inputEvents = new ArrayList<>(); // 构建数据源
//    Event event0 = new Event(0, "x", 1.0);
//    Event event1 = new Event(1, "a", 1.0);
//    Event event2 = new Event(2, "b", 2.0);
//    Event event3 = new Event(3, "c", 3.0);
//    Event event4 = new Event(4, "a", 4.0);
//    Event event5 = new Event(5, "b", 5.0);
//    inputEvents.add(new StreamRecord<>(event0, 3));
//    inputEvents.add(new StreamRecord<>(event1, 3));
//    inputEvents.add(new StreamRecord<>(event2, 3));
//    inputEvents.add(new StreamRecord<>(event3, 3));
//    inputEvents.add(new StreamRecord<>(event4, 3));
//    inputEvents.add(new StreamRecord<>(event5, 3));
//    //构建start状态的pattern1,过滤条件是事件名称以"a"开头
//    Pattern<Event, ?> pattern1 = Pattern.<Event>begin("pattern1")
//        .where(new SimpleCondition<Event>() {
//          private static final long serialVersionUID = 5726188262756267490L;
//
//          @Override
//          public boolean filter(Event value) throws Exception {
//            return value.getName().equals("a");
//          }
//        });
//    //构建end状态的pattern2,过滤条件是时间名称以"b"开头。其中pattern间是链式连接
//    Pattern<Event, ?> pattern2 = pattern1.next("pattern2")
//        .where(new SimpleCondition<Event>() {
//          private static final long serialVersionUID = 5726188262756267490L;
//
//          @Override
//          public boolean filter(Event value) throws Exception {
//            return value.getName().equals("b");
//          }
//        });
//
//    //将pattern编译成nfa
//    NFA<Event> nfa = NFACompiler
//        .compile(pattern2, Types.POJO(Event.class).createSerializer(env.getConfig()),
//            false);
//
//    //接入数据源到nfa,输出结果
//    List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);
//    resultingPatterns.forEach(list -> System.out
//        .println(list.stream().map(Event::toString).collect(Collectors.joining(","))));
//  }
//
//
//  @Test
//  public void testCEP3() {
//    final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
//
//    List<StreamRecord<Event>> inputEvents = new ArrayList<>(); // 构建数据源
//    Event event0 = new Event(0, "x", 1.0);
//    Event event1 = new Event(1, "a", 1.0);
//    Event event2 = new Event(2, "b", 2.0);
//    Event event3 = new Event(3, "c", 3.0);
//    Event event4 = new Event(4, "a", 4.0);
//    Event event5 = new Event(5, "b", 5.0);
//    inputEvents.add(new StreamRecord<>(event0, 3));
//    inputEvents.add(new StreamRecord<>(event1, 3));
//    inputEvents.add(new StreamRecord<>(event2, 3));
//    inputEvents.add(new StreamRecord<>(event3, 3));
//    inputEvents.add(new StreamRecord<>(event4, 3));
//    inputEvents.add(new StreamRecord<>(event5, 3));
//
//    //构建链接patterns
//    Pattern<Event, ?> pattern = Pattern.<Event>begin("start")
//        .where(new SimpleCondition<Event>() {
//          private static final long serialVersionUID = 5726188262756267490L;
//
//          @Override
//          public boolean filter(Event value) throws Exception {
//            return value.getName().equals("c");
//          }
//        }).followedBy("middle").where(new SimpleCondition<Event>() {
//          private static final long serialVersionUID = 5726188262756267490L;
//
//          @Override
//          public boolean filter(Event value) throws Exception {
//            return value.getName().equals("a");
//          }
//        })
//        .optional();
//    //创建nfa
//    NFA<Event> nfa = NFACompiler
//        .compile(pattern, Types.POJO(Event.class).createSerializer(env.getConfig()), false);
//    //接入数据源到nfa,输出结果
//    List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);
//    resultingPatterns.forEach(list -> System.out
//        .println(list.stream().map(Event::toString).collect(Collectors.joining(","))));
//  }
//
//  @Test
//  public void testCEP4() {
//    final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
//
//    List<StreamRecord<Event>> inputEvents = new ArrayList<>(); // 构建数据源
//    Event event0 = new Event(0, "x", 1.0);
//    Event event1 = new Event(1, "a", 1.0);
//    Event event2 = new Event(2, "b", 2.0);
//    Event event3 = new Event(3, "c", 3.0);
//    Event event4 = new Event(4, "a", 4.0);
//    Event event5 = new Event(5, "b", 5.0);
//    inputEvents.add(new StreamRecord<>(event0, 3));
//    inputEvents.add(new StreamRecord<>(event1, 3));
//    inputEvents.add(new StreamRecord<>(event2, 3));
//    inputEvents.add(new StreamRecord<>(event3, 3));
//    inputEvents.add(new StreamRecord<>(event4, 3));
//    inputEvents.add(new StreamRecord<>(event5, 3));
//
//    //构建链接patterns
//    Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
//      private static final long serialVersionUID = 5726188262756267490L;
//
//      @Override
//      public boolean filter(Event value) throws Exception {
//        return value.getName().equals("c");
//      }
//    }).followedBy("middle").where(new SimpleCondition<Event>() {
//      private static final long serialVersionUID = 5726188262756267490L;
//
//      @Override
//      public boolean filter(Event value) throws Exception {
//        return value.getName().equals("a");
//      }
//    }).optional();
//    //创建nfa
//    NFA<Event> nfa = NFACompiler
//        .compile(pattern, Types.POJO(Event.class).createSerializer(env.getConfig()), false);
//
//    //接入数据源到nfa,输出结果
//    List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);
//    resultingPatterns.forEach(list -> System.out
//        .println(list.stream().map(Event::toString).collect(Collectors.joining(","))));
//  }
//
//
//  @Test
//  public void testCEP5() {
//    final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
//
//    List<StreamRecord<Event>> inputEvents = new ArrayList<>(); // 构建数据源
//    Event event0 = new Event(40, "x", 1.0);
//    Event event1 = new Event(41, "c", 1.0);
//    Event event2 = new Event(42, "b", 2.0);
//    Event event3 = new Event(43, "b", 2.0);
//    Event event4 = new Event(44, "a", 2.0);
//    Event event5 = new Event(45, "b", 5.0);
//    inputEvents.add(new StreamRecord<>(event0, 3));
//    inputEvents.add(new StreamRecord<>(event1, 3));
//    inputEvents.add(new StreamRecord<>(event2, 3));
//    inputEvents.add(new StreamRecord<>(event3, 3));
//    inputEvents.add(new StreamRecord<>(event4, 3));
//    inputEvents.add(new StreamRecord<>(event5, 3));
//
//    //构建链接patterns
//    Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
//      private static final long serialVersionUID = 5726188262756267490L;
//
//      @Override
//      public boolean filter(Event value) throws Exception {
//        return value.getName().equals("c");
//      }
//    }).followedByAny("middle").where(new SimpleCondition<Event>() {
//      private static final long serialVersionUID = 5726188262756267490L;
//
//      @Override
//      public boolean filter(Event value) throws Exception {
//        return value.getName().equals("b");
//      }
//    }).optional();
//    //创建nfa
//    NFA<Event> nfa = NFACompiler
//        .compile(pattern, Types.POJO(Event.class).createSerializer(env.getConfig()), false);
//
//    //接入数据源到nfa,输出结果
//    List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);
//    resultingPatterns.forEach(list -> System.out
//        .println(list.stream().map(Event::toString).collect(Collectors.joining(","))));
//  }
//
//
//  @Test
//  public void testCEP6() {
//    //events表示事件数组
//    byte t = new Integer(1000).byteValue();
//    int b = t & 0xff;
//    System.out.println(b);
//  }
//}
