package com.luogh.learning.lab.simple;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class LambdaApp {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    SingleOutputStreamOperator<String> str = env.fromElements(1, 2, 3, 4)
        .map(ObjectB::new).name("map1")
        .map(Objects::toString).name("map2");
//        .map(x -> x.toString()) // 运行正常
    str.map(Objects::toString).name("map3") // 运行异常
        .print();

    JobGraph jobGraph = env.getStreamGraph().getJobGraph();

    env.getStreamGraph().getStreamNodes().forEach(node -> {
          TypeSerializer<?> type = node.getTypeSerializerOut();
          if (type != null) {
            System.out.println(
                node.getOperatorName() + "operator Id:" + node.getId()
                    + " ->  outputType:" + type.getClass().getCanonicalName());
          }

          TypeSerializer<?> inputType = node.getTypeSerializerIn1();
          if (inputType != null) {
            System.out.println(
                node.getOperatorName() + "operator Id:" + node.getId()
                    + " -> inputType:" + inputType.getClass().getCanonicalName());
          }
        }
    );

    for (JobVertex vertex : jobGraph.getVerticesSortedTopologicallyFromSources()) {
      String name = vertex.getName();
      System.out.println("\n JobVertex -> name:" + name);
      List<IntermediateDataSet> producedDataSets = vertex.getProducedDataSets();

      for (IntermediateDataSet prod : producedDataSets) {
        System.out.println("IntermediateDataSet -> resultType: " + prod.getResultType());
        System.out.println("IntermediateDataSet -> id:" + prod.getId());
      }

      List<JobEdge> inputs = vertex.getInputs();
      for (JobEdge edge : inputs) {
        System.out.println("JobEdge -> sourceId: " + edge.getSourceId());
        System.out.println("JobEdge -> distributionPattern: " + edge.getDistributionPattern());
        System.out.println("JobEdge -> shipStrategyName: " + edge.getShipStrategyName());
        System.out.println(
            "JobEdge -> preProcessingOperationName: " + edge.getPreProcessingOperationName());
      }

      System.out.println(
          "total operator ids in Vertex: " + name + " -> " + vertex.getOperatorIDs().stream()
              .map(Objects::toString).collect(Collectors.joining(",")));
    }
    env.execute("test");
  }


  public static class ObjectA {

  }

  public static class ObjectB {

    public ObjectB(Integer a) {
    }
  }

  @Getter
  @Setter
  public static class Apple {

    String f;
    String a;

    public Apple() {
    }

    public Apple(String f, String a) {
      this.f = f;
      this.a = a;
    }

    public static boolean nonNull(Apple a) {
      return Objects.nonNull(a);
    }
  }

}
