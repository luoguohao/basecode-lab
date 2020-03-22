package com.luogh.learning.lab.flink.cep;


import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CepTest implements Serializable {

    private static StreamExecutionEnvironment env;
    private OutputTag<Event> orderTimeoutOutput = new OutputTag<Event>("timeout") {
    };

    private static List<Event> events = new ArrayList<>();


    @BeforeClass
    public static void init() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        events.add(new Event("C", TimeUnit.SECONDS.toMillis(1L)));
        events.add(new Event("D", TimeUnit.SECONDS.toMillis(2L)));
        events.add(new Event("A1", TimeUnit.SECONDS.toMillis(3L)));
        events.add(new Event("C1", TimeUnit.SECONDS.toMillis(1L)));
        events.add(new Event("A2", TimeUnit.SECONDS.toMillis(4L)));
        events.add(new Event("A3", TimeUnit.SECONDS.toMillis(5L)));
        events.add(new Event("D", TimeUnit.SECONDS.toMillis(6L)));
        events.add(new Event("A4", TimeUnit.SECONDS.toMillis(7L)));
        events.add(new Event("B", TimeUnit.SECONDS.toMillis(8L)));
        events.add(new SubEvent("B1", TimeUnit.SECONDS.toMillis(8L), "SubB"));
    }

    @Test
    public void testWithin() throws Exception {
        DataStream<Event> stream = env.fromCollection(new DataSource(events), Event.class);
        Pattern<Event, Event> pattern = Pattern.<Event>begin("start")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().startsWith("C1");
                    }
                })
                .followedBy("middle")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().startsWith("A");
                    }
                })
                .followedBy("end")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().startsWith("B");
                    }
                }).within(Time.seconds(8));

        applyPattern(pattern, stream);
        env.execute();
    }

    @Test
    public void testNotFollowBy() throws Exception {
        // 模式序列不能以notFollowedBy（）结束
        DataStream<Event> stream = env.fromCollection(events);
        Pattern<Event, Event> pattern = Pattern.<Event>begin("start")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().startsWith("C");
                    }
                })
                .notFollowedBy("middle")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().startsWith("A1");
                    }
                })
                .followedBy("end")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().startsWith("B");
                    }
                });

        applyPattern(pattern, stream);
        env.execute();
    }

    @Test
    public void testUntil() throws Exception {
        DataStream<Event> stream = env.fromCollection(events);
        Pattern<Event, Event> pattern = Pattern.<Event>begin("start")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().startsWith("C");
                    }
                })
                .followedBy("middle")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().startsWith("A");
                    }
                })
                .oneOrMore()
                .until(new IterativeCondition<Event>() {
                    @Override
                    public boolean filter(Event value, Context<Event> ctx) throws Exception {
                        return value.getName().equals("A3");
                    }
                })
                .followedBy("end")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().startsWith("B");
                    }
                });

        applyPattern(pattern, stream);
        env.execute();
    }

    @Test
    public void testCombination() throws Exception {
        DataStream<Event> stream = env.fromCollection(events);
        Pattern<Event, Event> pattern = Pattern.<Event>begin("start")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().startsWith("C");
                    }
                })
                .followedBy("middle")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().startsWith("A");
                    }
                })
                .oneOrMore()
                .allowCombinations()
                .followedBy("end")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().startsWith("B");
                    }
                });

        applyPattern(pattern, stream);
        env.execute();
    }

    @Test
    public void testOr() throws Exception {
        DataStream<Event> stream = env.fromCollection(events);
        Pattern<Event, SubEvent> pattern = Pattern.<Event>begin("start")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().startsWith("C");
                    }
                })
                .followedByAny("middle") // 必须使用followByAny才能匹配or的关系
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().equals("A1");
                    }
                })
                .or(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getName().equals("A2");
                    }
                })
                .followedBy("end")
                .subtype(SubEvent.class)
                .where(new SimpleCondition<SubEvent>() {
                    @Override
                    public boolean filter(SubEvent value) throws Exception {
                        return value.getName().startsWith("B");
                    }
                });

        applyPattern(pattern, stream);
        env.execute();
    }

    private <T extends Event> void applyPattern(Pattern<Event, T> pattern, DataStream<Event> stream) {
        CEP.pattern(stream, pattern).flatSelect(
                orderTimeoutOutput,
                new PatternFlatTimeoutFunction<Event, Event>() {

                    @Override
                    public void timeout(Map<String, List<Event>> pattern, long timeoutTimestamp, Collector<Event> out) throws Exception {
                        List<Event> startEvents = pattern.getOrDefault("start", new ArrayList<>());
                        List<Event> middleEvents = pattern.getOrDefault("middle", new ArrayList<>());
                        List<Event> endEvents = pattern.getOrDefault("end", new ArrayList<>());
                        System.err.println("timeout pattern: ############> start:" + startEvents + ", middle:" + middleEvents + ", end:" + endEvents);

                    }
                },
                new PatternFlatSelectFunction<Event, Event>() {

                    @Override
                    public void flatSelect(Map<String, List<Event>> pattern, Collector<Event> out) throws Exception {
                        List<Event> startEvents = pattern.getOrDefault("start", new ArrayList<>());
                        List<Event> middleEvents = pattern.getOrDefault("middle", new ArrayList<>());
                        List<Event> endEvents = pattern.getOrDefault("end", new ArrayList<>());
                        System.err.println("success pattern: ==========> start:" + startEvents + ", middle:" + middleEvents + ", end:" + endEvents);
                    }
                }

        ).addSink(new NothingSink());
    }

    @Test
    public void testSubType() throws Exception {
        DataStream<Event> stream = env.fromCollection(events);

        Pattern<Event, SubEvent> pattern = Pattern.<Event>begin("start")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().startsWith("C");
                    }
                })
                .followedBy("middle").where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().startsWith("A");
                    }
                }).oneOrMore()  // 尽可能多的匹配
                .followedBy("end")
                .subtype(SubEvent.class)
                .where(new SimpleCondition<SubEvent>() {
                    @Override
                    public boolean filter(SubEvent value) throws Exception {
                        return value.getName().startsWith("B");
                    }
                });

        CEP.pattern(stream, pattern).flatSelect(
                orderTimeoutOutput,
                new PatternFlatTimeoutFunction<Event, Event>() {

                    @Override
                    public void timeout(Map<String, List<Event>> pattern, long timeoutTimestamp, Collector<Event> out) throws Exception {

                    }
                },
                new PatternFlatSelectFunction<Event, Event>() {

                    @Override
                    public void flatSelect(Map<String, List<Event>> pattern, Collector<Event> out) throws Exception {
                        List<Event> startEvents = pattern.getOrDefault("start", new ArrayList<>());
                        List<Event> middleEvents = pattern.getOrDefault("middle", new ArrayList<>());
                        List<Event> endEvents = pattern.getOrDefault("end", new ArrayList<>());
                        System.err.println("success pattern: ==========> start:" + startEvents + ", middle:" + middleEvents + ", end:" + endEvents);
                    }
                }

        ).addSink(new NothingSink());

        env.execute();
    }

    @Test
    public void testGreedy() throws Exception {
        DataStream<Event> stream = env.fromCollection(events);
        Pattern<Event, Event> pattern = Pattern.<Event>begin("start")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().startsWith("C");
                    }
                })
                .followedBy("middle").where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().startsWith("A");
                    }
                }).oneOrMore().greedy()  // 尽可能多的匹配
                .followedBy("end").where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().startsWith("B");
                    }
                });

        CEP.pattern(stream, pattern).flatSelect(
                orderTimeoutOutput,
                new PatternFlatTimeoutFunction<Event, Event>() {

                    @Override
                    public void timeout(Map<String, List<Event>> pattern, long timeoutTimestamp, Collector<Event> out) throws Exception {

                    }
                },
                new PatternFlatSelectFunction<Event, Event>() {

                    @Override
                    public void flatSelect(Map<String, List<Event>> pattern, Collector<Event> out) throws Exception {
                        List<Event> startEvents = pattern.getOrDefault("start", new ArrayList<>());
                        List<Event> middleEvents = pattern.getOrDefault("middle", new ArrayList<>());
                        List<Event> endEvents = pattern.getOrDefault("end", new ArrayList<>());
                        System.err.println("success pattern: ==========> start:" + startEvents + ", middle:" + middleEvents + ", end:" + endEvents);
                    }
                }

        ).addSink(new NothingSink());

        env.execute();
    }

    @Test
    public void testConsecutive() throws Exception {
        DataStream<Event> stream = env.fromCollection(events);
        Pattern<Event, Event> pattern = Pattern.<Event>begin("start")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().startsWith("C");
                    }
                })
                .followedBy("middle").where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().startsWith("A");
                    }
                }).oneOrMore().consecutive() // 匹配必须是连续的
                .followedBy("end").where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().startsWith("B");
                    }
                });


        CEP.pattern(stream, pattern).flatSelect(
                orderTimeoutOutput,
                new PatternFlatTimeoutFunction<Event, Event>() {

                    @Override
                    public void timeout(Map<String, List<Event>> pattern, long timeoutTimestamp, Collector<Event> out) throws Exception {

                    }
                },
                new PatternFlatSelectFunction<Event, Event>() {

                    @Override
                    public void flatSelect(Map<String, List<Event>> pattern, Collector<Event> out) throws Exception {
                        List<Event> startEvents = pattern.getOrDefault("start", new ArrayList<>());
                        List<Event> middleEvents = pattern.getOrDefault("middle", new ArrayList<>());
                        List<Event> endEvents = pattern.getOrDefault("end", new ArrayList<>());
                        System.err.println("success pattern 1: ==========> start:" + startEvents + ", middle:" + middleEvents + ", end:" + endEvents);
                    }
                }

        ).addSink(new NothingSink());

        Pattern<Event, Event> pattern1 = Pattern.<Event>begin("start")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().startsWith("C");
                    }
                })
                .followedBy("middle").where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().startsWith("A");
                    }
                }).oneOrMore()
                .followedBy("end").where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().startsWith("B");
                    }
                });


        CEP.pattern(stream, pattern1).flatSelect(
                orderTimeoutOutput,
                new PatternFlatTimeoutFunction<Event, Event>() {

                    @Override
                    public void timeout(Map<String, List<Event>> pattern, long timeoutTimestamp, Collector<Event> out) throws Exception {

                    }
                },
                new PatternFlatSelectFunction<Event, Event>() {

                    @Override
                    public void flatSelect(Map<String, List<Event>> pattern, Collector<Event> out) throws Exception {
                        List<Event> startEvents = pattern.getOrDefault("start", new ArrayList<>());
                        List<Event> middleEvents = pattern.getOrDefault("middle", new ArrayList<>());
                        List<Event> endEvents = pattern.getOrDefault("end", new ArrayList<>());
                        System.err.println("success pattern 2: ==========> start:" + startEvents + ", middle:" + middleEvents + ", end:" + endEvents);
                    }
                }

        ).addSink(new NothingSink());

        env.execute();
    }

    public static class NothingSink implements SinkFunction<Event> {

    }


    public static class DataSource implements Iterator<Event>, Serializable {
        private final AtomicInteger atomicInteger = new AtomicInteger(0);
        private final List<Event> orderEventList;

        public DataSource(List<Event> orderEventList) {
            this.orderEventList = orderEventList;
        }

        @Override
        public boolean hasNext() {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return atomicInteger.getAndIncrement() < orderEventList.size();
        }

        @Override
        public Event next() {
            return orderEventList.get(atomicInteger.get() - 1);
        }
    }
}
