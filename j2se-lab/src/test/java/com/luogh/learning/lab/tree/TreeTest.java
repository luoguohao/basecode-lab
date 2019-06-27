package com.luogh.learning.lab.tree;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.luogh.learning.lab.tree.Tree.NodeValeAggregator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 3)
@Measurement(iterations = 10, time = 5, timeUnit = TimeUnit.SECONDS)
@Threads(8)
@Fork(2)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(value = Scope.Benchmark)
public class TreeTest {

  private List<OriginData> singleNodeList = Lists.newArrayList();

  @Setup
  public void init() {
    singleNodeList.add(new OriginData(-1, 0, null, 0, "测试:1", "1", null));
    singleNodeList.add(new OriginData(1, 0, -1, 0, "测试:1", "1",
        ImmutableMap.of(CommonConstants.TREE_NODE_ITEM_COUNT_KEY, 0)));
    singleNodeList.add(new OriginData(1, 1, -1, 0, "测试:1-类型1", "1",
        ImmutableMap.of(CommonConstants.TREE_NODE_ITEM_COUNT_KEY, 2)));
    singleNodeList.add(new OriginData(2, 0, 1, 0, "测试:1-2", "1-2", null));
    singleNodeList.add(new OriginData(3, 1, 1, 0, "测试:1-3-类型1", "1-3", null));
    singleNodeList.add(new OriginData(3, 0, 1, 0, "测试:1-3-类型0", "1-3",
        ImmutableMap.of(CommonConstants.TREE_NODE_ITEM_COUNT_KEY, 2)));
    singleNodeList.add(new OriginData(4, 0, 5, 0, "测试:1-4", "1-4",
        ImmutableMap.of(CommonConstants.TREE_NODE_ITEM_COUNT_KEY, 3)));
    singleNodeList.add(new OriginData(5, 0, 2, 0, "测试:1-5", "1-5",
        ImmutableMap.of(CommonConstants.TREE_NODE_ITEM_COUNT_KEY, 4)));
  }

  @Benchmark
  public void toTreeJson() throws Exception {
    Tree<OriginData, Integer, String> tree = new Tree.Builder<>(-1, 0, "树测试", singleNodeList)
        .withNodeId(OriginData::getId)
        .withNodeIdType(OriginData::getIdType)
        .withParentNodeId(OriginData::getParentId)
        .withParentNodeIdType(OriginData::getParentIdType)
        .withValue(OriginData::getText)
        .withHierarchyId(OriginData::getHierarchyId)
        .withAdditionalProperties(OriginData::getAdditionalParams)
        .addNodeItemAggregator(new SumAggregator())
        .build();
    System.out.println(JSON.toJSONString(tree.getRootTreeNode()));
  }

  @Getter
  @AllArgsConstructor
  static class OriginData {

    private Integer id;
    private Integer idType;
    private Integer parentId;
    private Integer parentIdType;
    private String text;
    private String hierarchyId;
    private Map<String, Object> additionalParams;
  }

  public class SumAggregator implements NodeValeAggregator {

    @Override
    public String fieldName() {
      return CommonConstants.TREE_NODE_ITEM_COUNT_KEY;
    }

    @Override
    public Object defaultValueIfNotExistOrNull() {
      return 0;
    }

    @Override
    public Object reduceBy(Object currentValue, Object delta) {
      Preconditions.checkArgument(currentValue != null && delta != null,
          "invalid input params, must not be null");
      Integer cValue = (Integer) currentValue;
      Integer deltaValue = (Integer) delta;
      return cValue + deltaValue;
    }
  }

}
