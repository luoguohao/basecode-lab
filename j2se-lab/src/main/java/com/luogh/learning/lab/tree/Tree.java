package com.luogh.learning.lab.tree;

import static java.util.Optional.empty;
import static java.util.stream.Collectors.groupingByConcurrent;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;


/**
 * 根据指定节点id和节点idType和值来生成树形结构，通过使用节点id和节点idType来进行树节点的唯一标识 因为考虑到树中的节点id可能来自不同的数据源：比如人群/特性运营组中架构树中包含运营组id以及目录id两种类型
 *
 * @param <DATA> 原始数据类型
 * @param <KEY> 树形节点Id类型
 * @param <VALUE> 树形节点数据类型
 */
@Slf4j
public class Tree<DATA, KEY extends Comparable<KEY>, VALUE extends Comparable<VALUE>> {

  private final NodeIdentifier<KEY> rootNodeIdentifier;
  private final AtomicInteger currentLevel; // 当前树层级
  private final List<DATA> treeData;  // 原始数据
  private final VALUE rootNodeValue; // 跟节点值

  private Function<DATA, KEY> nodeIdSupplier;
  private Function<DATA, KEY> parentNodeIdSupplier;
  private Function<DATA, Integer> parentNodeIdTypeSupplier;
  private Function<DATA, VALUE> valueSupplier;
  private Function<DATA, Integer> nodeTypeSupplier;


  private Optional<Function<DATA, Map<String, Object>>> additionalPropertiesSupplier;
  private Optional<Predicate<DATA>> isCheckedSupplier;
  private Optional<Function<DATA, String>> hierarchyIdSupplier;
  private Map<String, NodeValeAggregator> nodeValueAggregators; // 额外参数的需要进行聚合的聚合函数

  private NodeIdentifier<KEY> vNodeIdentifier; // 虚拟节点id，用于记录原始数据中的指定根节点的parentId

  /**
   * 根节点
   */
  @Getter
  private TreeNode<KEY, VALUE> rootTreeNode;
  /**
   * 根据nodeId和parentNodeId构建的原始数据依赖关系,表示指定Id被哪些id所依赖，即可认为 是指定ID下的所有子ID,
   * 因为存在不同类型的id,所以使用tuple来分别记录id和id类型
   */
  private Map<NodeIdentifier<KEY>, Set<NodeIdentifier<KEY>>> idRelations;
  /**
   * 获取节点的直接父级节点
   */
  private Map<NodeIdentifier<KEY>, NodeIdentifier<KEY>> directParentRelations;
  /**
   * 获取节点的所有祖先节点
   */
  private Map<NodeIdentifier<KEY>, Set<NodeIdentifier<KEY>>> reversedIdRelations;
  /**
   * 当前所有的节点
   */
  private Map<NodeIdentifier<KEY>, TreeNode<KEY, VALUE>> allNodes;

  private Tree(NodeIdentifier<KEY> rootNodeIdentifier, VALUE rootNodeValue, List<DATA> originData) {
    this.rootNodeIdentifier = rootNodeIdentifier;
    this.rootNodeValue = rootNodeValue;
    this.currentLevel = new AtomicInteger(0);
    this.additionalPropertiesSupplier = empty();
    this.isCheckedSupplier = empty();
    this.hierarchyIdSupplier = empty();
    this.treeData = originData;
    this.nodeValueAggregators = Maps.newHashMap();
  }

  /**
   * 开始构建树
   */
  private void construct() {
    this.allNodes = buildAllNodes();       // build all nodes
    this.rootTreeNode = getRootNode();     // create root node
    this.idRelations = buildIdRelations(); // build id relationship
    this.directParentRelations = buildDirectParentRelations(this.idRelations);
    this.relationshipCheck(this.idRelations, this.directParentRelations);
    this.reversedIdRelations = buildIdReversedRelations(this.idRelations,
        this.directParentRelations);
    this.treeInRecurse(this.currentLevel, this.rootNodeIdentifier, this.rootTreeNode);
  }

  /**
   * 依赖关系检查,防止循环依赖
   */
  private void relationshipCheck(Map<NodeIdentifier<KEY>, Set<NodeIdentifier<KEY>>> idRelations,
      Map<NodeIdentifier<KEY>, NodeIdentifier<KEY>> directParentRelations) {
    Preconditions.checkArgument(directParentRelations != null && idRelations != null);
    if (vNodeIdentifier != null) {
      Preconditions.checkState(!directParentRelations.containsKey(vNodeIdentifier),
          "cycle dependency found for the node:%s", vNodeIdentifier);
    }
    // 检查父子关系与子父关系不重复
    Set<Tuple<NodeIdentifier<KEY>, NodeIdentifier<KEY>>> directRel = directParentRelations
        .entrySet()
        .parallelStream()
        .map(entry -> new Tuple<>(entry.getKey(), entry.getValue()))
        .collect(toSet());

    Set<Tuple<NodeIdentifier<KEY>, NodeIdentifier<KEY>>> cycleDependency = idRelations.entrySet()
        .parallelStream()
        .flatMap(entry -> entry.getValue().parallelStream()
            .map(value -> new Tuple<>(entry.getKey(), value)))
        .filter(directRel::contains)
        .collect(toSet());

    if (cycleDependency.size() > 0) {
      throw new RuntimeException(
          "cycle dependency found, with the following cycle dependency:%s" + cycleDependency);
    }
  }

  private TreeNode<KEY, VALUE> getRootNode() {
    return this.allNodes.get(this.rootNodeIdentifier);
  }


  private void treeInRecurse(AtomicInteger currentLevel, NodeIdentifier<KEY> currentNodeKey,
      TreeNode<KEY, VALUE> currentNode) {
    // set tree node level
    currentNode.setLevel(currentLevel.getAndIncrement());
    // get all children tree node
    Set<NodeIdentifier<KEY>> childNodeKeys = this.idRelations.getOrDefault(currentNodeKey,
        Sets.newHashSet());

    for (NodeIdentifier<KEY> childNodeKey : childNodeKeys) {
      TreeNode<KEY, VALUE> childNode = Preconditions.checkNotNull(this.allNodes.get(childNodeKey));
      childNode.setLevel(currentLevel.get());
      currentNode.addChild(childNode);
      // if childNode has children, recurse
      if (this.idRelations.containsKey(childNodeKey)) {
        treeInRecurse(currentLevel, childNodeKey, childNode);
      } else {
        // is a leaf data, increment leaf node cnt for all ancestors
        // get all ancestors
        Set<NodeIdentifier<KEY>> ancestors = this.reversedIdRelations.getOrDefault(currentNodeKey,
            Sets.newHashSet());
        for (NodeIdentifier<KEY> ancestor : ancestors) {
          TreeNode<KEY, VALUE> ancestorNode = this.allNodes.get(ancestor);
          Preconditions.checkNotNull(ancestorNode, "not found ancestor info:" + ancestor);
          ancestorNode.incrementLeafNodeCntBy(1);
        }
      }

      // eval aggregator
      if (this.nodeValueAggregators.size() > 0) {
        nodeValueAggregateEval(childNodeKey, currentNodeKey);
      }
    }
  }

  private void nodeValueAggregateEval(NodeIdentifier<KEY> currentNodeIdentifier,
      NodeIdentifier<KEY> parentNodeIdentifier) {
    Preconditions.checkArgument(parentNodeIdentifier != null);
    TreeNode<KEY, VALUE> currentNode = this.allNodes.get(currentNodeIdentifier);
    Map<String, Optional<Object>> additionalParams = currentNode._additionProperties;
    NodeValeAggregator nodeValeAggregator;
    for (Entry<String, NodeValeAggregator> entry : this.nodeValueAggregators.entrySet()) {
      nodeValeAggregator = entry.getValue();
      Object currentValue = getValueOrElse(additionalParams, entry.getKey(),
          nodeValeAggregator.defaultValueIfNotExistOrNull());
      Object currentAncestorValue;
      TreeNode<KEY, VALUE> parentNode = this.allNodes.get(parentNodeIdentifier);
      Preconditions.checkNotNull(parentNode, "not found ancestor info:" + parentNode);

      currentAncestorValue = getValueOrElse(parentNode._additionProperties, entry.getKey(),
          nodeValeAggregator.defaultValueIfNotExistOrNull());
      Object newAncestorValue = nodeValeAggregator.reduceBy(currentAncestorValue, currentValue);
      Preconditions
          .checkArgument(newAncestorValue != null, "aggregated filed result value can not be null");
      // put new data
      parentNode._additionProperties.put(entry.getKey(), Optional.ofNullable(newAncestorValue));
    }
  }


  private Object getValueOrElse(Map<String, Optional<Object>> map, String key,
      Object defaultValue) {
    Preconditions.checkArgument(map != null && key != null && defaultValue != null);
    if (map.containsKey(key)) { // 如果存在，但是值为null,依然使用默认值
      return map.get(key).orElse(defaultValue);
    } else { // 如果不存在，使用默认值
      return defaultValue;
    }
  }


  /**
   * 根据nodeId和parentNodeId找到原始数据中的所有Id父子依赖关系
   */
  private Map<NodeIdentifier<KEY>, Set<NodeIdentifier<KEY>>> buildIdRelations() {

    Map<NodeIdentifier<KEY>, Set<NodeIdentifier<KEY>>> idRelations = treeData.parallelStream()
        .collect(
            groupingByConcurrent(
                nodeIdentifier(parentNodeIdSupplier, parentNodeIdTypeSupplier)
                    .andThen(this::checkIsValidIdentifier),
                mapping(nodeIdentifier(nodeIdSupplier, nodeTypeSupplier)
                    .andThen(this::checkIsValidIdentifier), toSet())
            )
        );

    if (idRelations.size() > 0) { // maybe an empty tree
      Preconditions.checkState(idRelations.containsKey(this.rootNodeIdentifier),
          "not found tree root node id %s in the parentId->Set[nodeId] relationship:%s",
          this.rootNodeIdentifier, idRelations);
    }
    return idRelations;
  }

  private NodeIdentifier<KEY> checkIsValidIdentifier(NodeIdentifier<KEY> nodeIdentifier) {
    boolean isRootNode = this.rootNodeIdentifier.equals(nodeIdentifier);
    Preconditions.checkState(this.allNodes.get(nodeIdentifier) != null || isRootNode,
        "invalid NodeIdentifier %s not found in the parentId->Set[nodeId] relationship.",
        nodeIdentifier);
    return nodeIdentifier;
  }

  /**
   * 根据父子关系找到原始数据中的所有Id子父依赖关系
   */
  private Map<NodeIdentifier<KEY>, Set<NodeIdentifier<KEY>>> buildIdReversedRelations(
      Map<NodeIdentifier<KEY>, Set<NodeIdentifier<KEY>>> pcIdRelation,
      Map<NodeIdentifier<KEY>, NodeIdentifier<KEY>> directParentRelations) {
    Preconditions.checkArgument(pcIdRelation != null && directParentRelations != null);
    // 首先查询所有节点的直接上级
    Map<NodeIdentifier<KEY>, Set<NodeIdentifier<KEY>>> reversedRelations = directParentRelations
        .entrySet().parallelStream()
        .map(entry -> new Tuple<>(entry.getKey(),
            idReversedRelationsInRecurse(directParentRelations, entry.getKey(),
                Sets.newConcurrentHashSet())))
        .collect(toMap(Tuple::getKey, Tuple::getValue));
    return reversedRelations;
  }

  /**
   * 根据父级节点关系，获得所有节点的直接上级节点
   */
  private Map<NodeIdentifier<KEY>, NodeIdentifier<KEY>> buildDirectParentRelations(
      Map<NodeIdentifier<KEY>, Set<NodeIdentifier<KEY>>> pcIdRelation) {
    return pcIdRelation.entrySet()
        .parallelStream()
        .flatMap(
            entry -> entry.getValue().parallelStream().map(e -> new Tuple<>(e, entry.getKey())))
        .collect(toMap(Tuple::getKey, Tuple::getValue));
  }

  /**
   * 找到每一个节点的所有祖先
   *
   * @param allDirectParentRel 当前所有的直接上级依赖关系
   * @param nodeIdentifier 需要查询的节点
   * @param allAncestors 每次查找得到的祖先节点
   */
  private Set<NodeIdentifier<KEY>> idReversedRelationsInRecurse(
      Map<NodeIdentifier<KEY>, NodeIdentifier<KEY>> allDirectParentRel,
      NodeIdentifier<KEY> nodeIdentifier,
      Set<NodeIdentifier<KEY>> allAncestors) {
    Preconditions.checkArgument(nodeIdentifier != null);
    NodeIdentifier<KEY> directParentNode = allDirectParentRel.get(nodeIdentifier);
    if (nodeIdentifier.equals(this.rootNodeIdentifier)) { // 如果当前节点是根节点，就直接返回，不往上查询
      return allAncestors;
    } else {
      if (directParentNode != null) {
        allAncestors.add(directParentNode);
        return idReversedRelationsInRecurse(allDirectParentRel, directParentNode, allAncestors);
      } else {
        return allAncestors;
      }
    }
  }


  private Map<NodeIdentifier<KEY>, TreeNode<KEY, VALUE>> buildAllNodes() {
    Map<NodeIdentifier<KEY>, TreeNode<KEY, VALUE>> allNodes = treeData.parallelStream()
        .collect(toMap(nodeIdentifier(nodeIdSupplier, nodeTypeSupplier), buildTreeNode()));

    Preconditions.checkState(allNodes.size() == treeData.size(), "invalid id relationship build.");

    // 查询是否有节点处于选中状态，如果没有，默认选中根节点
    boolean isChecked = allNodes.entrySet().parallelStream()
        .anyMatch(entry -> entry.getValue().isChecked);

    if (!allNodes.containsKey(this.rootNodeIdentifier)) {
      log.warn("origin data not contains root node info, using default root node info");
      this.rootTreeNode = new TreeNode<>(this.rootNodeIdentifier.getKey(),
          this.rootNodeIdentifier.getValue(), this.rootNodeValue, !isChecked);

      allNodes.put(this.rootNodeIdentifier, this.rootTreeNode);
    } else {
      TreeNode<KEY, VALUE> rootTreeNode = allNodes.get(this.rootNodeIdentifier);
      NodeIdentifier<KEY> vNode = new NodeIdentifier<>(rootTreeNode.parentNodeId,
          rootTreeNode.parentNodeIdType); // 虚拟节点，记录根节点的根节点
      this.vNodeIdentifier = vNode;
      allNodes.put(vNode, new TreeNode<>(vNode));
    }
    return allNodes;
  }


  private Function<DATA, NodeIdentifier<KEY>> nodeIdentifier(Function<DATA, KEY> nodeIdSupplier,
      Function<DATA, Integer> nodeIdTypeSupplier) {
    return data -> new NodeIdentifier<>(nodeIdSupplier.apply(data), nodeIdTypeSupplier.apply(data));
  }


  private Function<DATA, TreeNode<KEY, VALUE>> buildTreeNode() {
    return t -> new TreeNode<>(nodeIdSupplier.apply(t),
        nodeTypeSupplier.apply(t),
        parentNodeIdSupplier.apply(t),
        parentNodeIdTypeSupplier.apply(t),
        isCheckedSupplier.orElseGet(this::alwaysFalse).test(t),
        hierarchyIdSupplier.orElseGet(this::alwaysNull).apply(t),
        valueSupplier.apply(t),
        additionalPropertiesSupplier.orElseGet(this::alwaysNull).apply(t)
    );
  }


  private Predicate<DATA> alwaysFalse() {
    return t -> Predicates.alwaysFalse().apply(t);
  }

  private <R> Function<DATA, R> alwaysNull() {
    return t -> null;
  }


  public static final class Builder<DATA, KEY extends Comparable<KEY>, VALUE extends Comparable<VALUE>> {

    private final Tree<DATA, KEY, VALUE> tree;

    /**
     * @param rootNodeId 根节点的nodeId
     * @param rootNodeIdType 根节点的nodeId Type
     */
    public Builder(KEY rootNodeId, Integer rootNodeIdType, VALUE rootNodeValue,
        List<DATA> treeData) {
      Preconditions.checkArgument(treeData != null && rootNodeId != null && rootNodeIdType != null
          && rootNodeValue != null);
      this.tree = new Tree<>(new NodeIdentifier<>(rootNodeId, rootNodeIdType), rootNodeValue,
          Lists.newCopyOnWriteArrayList(treeData));
    }


    public Tree<DATA, KEY, VALUE> build() {
      Preconditions.checkArgument(this.tree.nodeIdSupplier != null, "nodeIdSupplier required.");
      Preconditions
          .checkArgument(this.tree.parentNodeIdSupplier != null, "parentNodeIdSupplier required.");
      Preconditions.checkArgument(this.tree.valueSupplier != null, "valueSupplier required.");
      Preconditions.checkArgument(this.tree.nodeTypeSupplier != null, "nodeTypeSupplier required.");
      Preconditions.checkArgument(this.tree.parentNodeIdTypeSupplier != null,
          "parentNodeIdTypeSupplier required.");
      this.tree.construct();
      return this.tree;
    }


    public Builder<DATA, KEY, VALUE> withNodeId(Function<DATA, KEY> nodeIdSupplier) {
      Preconditions.checkArgument(nodeIdSupplier != null);
      this.tree.nodeIdSupplier = nodeIdSupplier;
      return this;
    }

    public Builder<DATA, KEY, VALUE> withParentNodeId(Function<DATA, KEY> parentNodeIdSupplier) {
      Preconditions.checkArgument(parentNodeIdSupplier != null);
      this.tree.parentNodeIdSupplier = parentNodeIdSupplier;
      return this;
    }

    public Builder<DATA, KEY, VALUE> withParentNodeIdType(
        Function<DATA, Integer> parentNodeIdTypeSupplier) {
      Preconditions.checkArgument(parentNodeIdTypeSupplier != null);
      this.tree.parentNodeIdTypeSupplier = parentNodeIdTypeSupplier;
      return this;
    }

    public Builder<DATA, KEY, VALUE> withNodeIdType(Function<DATA, Integer> nodeTypeSupplier) {
      Preconditions.checkArgument(nodeTypeSupplier != null);
      this.tree.nodeTypeSupplier = nodeTypeSupplier;
      return this;
    }

    public Builder<DATA, KEY, VALUE> withValue(Function<DATA, VALUE> nodeValueSupplier) {
      Preconditions.checkArgument(nodeValueSupplier != null);
      this.tree.valueSupplier = nodeValueSupplier;
      return this;
    }

    public Builder<DATA, KEY, VALUE> withAdditionalProperties(
        Function<DATA, Map<String, Object>> additionalPropertiesSupplier) {
      this.tree.additionalPropertiesSupplier = Optional.ofNullable(additionalPropertiesSupplier);
      return this;
    }

    public Builder<DATA, KEY, VALUE> withIsChecked(Predicate<DATA> isCheckedSupplier) {
      this.tree.isCheckedSupplier = Optional.ofNullable(isCheckedSupplier);
      return this;
    }

    public Builder<DATA, KEY, VALUE> withHierarchyId(Function<DATA, String> hierarchyIdSupplier) {
      this.tree.hierarchyIdSupplier = Optional.ofNullable(hierarchyIdSupplier);
      return this;
    }

    public Builder<DATA, KEY, VALUE> addNodeItemAggregator(
        NodeValeAggregator nodeValeAggregator) {
      Preconditions
          .checkArgument(nodeValeAggregator != null && nodeValeAggregator.fieldName() != null);

      String fieldName = nodeValeAggregator.fieldName();
      log.debug("add new nodeValeAggregator for node:{}", fieldName);
      if (this.tree.nodeValueAggregators.containsKey(fieldName)) {
        log.warn("already exist aggregator for the key:{}, will override it.", fieldName);
      }
      this.tree.nodeValueAggregators.put(fieldName, nodeValeAggregator);
      return this;
    }
  }

  /**
   * 树节点的唯一标识
   *
   * @param <KEY> nodeId类型
   */
  @Getter
  private final static class NodeIdentifier<KEY> extends Tuple<KEY, Integer> {

    /**
     * 通过nodeId和nodeIdType两个字段来唯一标识
     *
     * @param nodeId nodeId
     * @param nodeIdType nodeIdType类型
     */
    public NodeIdentifier(KEY nodeId, Integer nodeIdType) {
      super(nodeId, nodeIdType);
    }

    @Override
    public String toString() {
      return "NodeIdentifier{" +
          "key=" + key +
          ", keyType=" + value +
          '}';
    }
  }

  /**
   * @param <KEY> 节点的ID类型
   * @param <VALUE> 节点的值类型
   */
  @ToString
  public static final class TreeNode<KEY extends Comparable<KEY>, VALUE extends Comparable<VALUE>>
      implements Comparable<TreeNode<KEY, VALUE>> {

    private final KEY nodeId;

    private final Integer nodeIdType;

    private final KEY parentNodeId;

    private final Integer parentNodeIdType;

    private final Boolean isChecked;

    private final String hierarchyId;
    private final VALUE text;
    private Integer level;


    private final ConcurrentMap<String, Optional<Object>> _additionProperties; // 使用concurrentHashMap,防止并发修改
    private List<TreeNode<KEY, VALUE>> children;

    private Integer leafNodeCnt;

    public TreeNode(NodeIdentifier<KEY> nodeIdentifier) {
      this(nodeIdentifier.getKey(), nodeIdentifier.getValue(), null, null, false, "", null, null);
    }

    public TreeNode(KEY nodeId, Integer nodeIdType, VALUE text, Boolean isChecked) {
      this(nodeId, nodeIdType, null, null, isChecked, "", text, null);
    }

    public TreeNode(KEY nodeId, Integer nodeIdType, KEY parentNodeId, Integer parentNodeIdType,
        Boolean isChecked, String hierarchyId, VALUE text,
        Map<String, Object> additionProperties) {
      this.nodeId = nodeId;
      this.nodeIdType = nodeIdType;
      this.parentNodeId = parentNodeId;
      this.parentNodeIdType = parentNodeIdType;
      this.isChecked = isChecked;
      this.hierarchyId = hierarchyId;
      this.text = text;
      this._additionProperties = Maps.newConcurrentMap();
      this.leafNodeCnt = 0;
      this.children = Lists.newLinkedList();
      if (additionProperties != null) {
        // for the sake of null value, ConcurrentHashMap can not support null key or value
        // but hashMap support, see @{https://blog.csdn.net/gagewang1/article/details/54971965}
        additionProperties.entrySet()
            .forEach(entry ->
                this._additionProperties.put(entry.getKey(), Optional.ofNullable(entry.getValue()))
            );
      }
    }

    /**
     * 所有子节点
     */
    public List<TreeNode<KEY, VALUE>> getChildren() {
      return this.children.parallelStream().sorted().collect(toList());
    }

    /**
     * 增加子节点
     */
    public void addChild(TreeNode<KEY, VALUE> treeNode) {
      this.children.add(treeNode);
    }

    /**
     * 增加子节点
     */
    public void addChildren(List<TreeNode<KEY, VALUE>> treeNodes) {
      this.children.addAll(treeNodes);
    }


    /**
     * 下级所有叶子节点数量
     */
    public void incrementLeafNodeCntBy(int incr) {
      this.leafNodeCnt += incr;
    }

    /**
     * 子节点个数：children列表的大小
     */
    public Integer childrenCnt() {
      return this.children.size();
    }

    /**
     * 是否叶子节点
     */

    public Boolean getIsLeaf() {
      return this.children.size() == 0;
    }

    public Map<String, Object> additionProperties() {
      Map<String, Object> hashMaps = Maps.newHashMap();
      // convert to hashMap, if origin is null, set to null
      for (Map.Entry<String, Optional<Object>> entry : this._additionProperties.entrySet()) {
        hashMaps.put(entry.getKey(), entry.getValue().orElse(null));
      }
      return hashMaps;
    }

    public KEY getNodeId() {
      return nodeId;
    }

    public Integer getNodeIdType() {
      return nodeIdType;
    }

    public KEY getParentNodeId() {
      return parentNodeId;
    }

    public Integer getParentNodeIdType() {
      return parentNodeIdType;
    }

    public Boolean getChecked() {
      return isChecked;
    }

    public String getHierarchyId() {
      return hierarchyId;
    }

    public VALUE getText() {
      return text;
    }

    public Integer getLeafNodeCnt() {
      return leafNodeCnt;
    }

    public Integer getLevel() {
      return level;
    }

    public void setLevel(Integer level) {
      this.level = level;
    }

    @Override
    public int compareTo(TreeNode<KEY, VALUE> o) {
      int idTypeComp = this.nodeIdType.compareTo(o.getNodeIdType());
      if (idTypeComp == 0) {
        int leaf = this.getIsLeaf().compareTo(o.getIsLeaf());
        return leaf == 0 ? this.getText().compareTo(o.getText()) : leaf;
      } else {
        return idTypeComp;
      }
    }
  }

  /**
   * 自定义参数中需要进行汇聚的字段的聚合逻辑函数
   */
  public interface NodeValeAggregator {

    /**
     * 指定需要聚合的字段名称
     */
    String fieldName();

    /**
     * 如果当前node不包含该参数，指定返回默认的值
     */
    Object defaultValueIfNotExistOrNull();

    /**
     * 执行的聚合操作
     *
     * @param currentValue 当前节点的值
     * @param delta 需要加入的值
     */
    Object reduceBy(Object currentValue, Object delta);
  }

  public final static class SumAggregator implements NodeValeAggregator {

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
