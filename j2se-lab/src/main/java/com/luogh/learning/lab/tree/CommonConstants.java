package com.luogh.learning.lab.tree;

/**
 * 公共常量
 */
public interface CommonConstants {

  /**
   * 树根节点的id默认值
   */
  String TREE_ROOT_NODE_ID = "0";

  /**
   * 部门结构树中的渠道运营节点名称
   */
  String TREE_NODE_DEPT_CHANNEL_OP_NAME = "渠道运营";
  /**
   * 树结构中依赖的额外参数名称，用于统计树节点下有多少记录数
   */
  String TREE_NODE_ITEM_COUNT_KEY = "itemCount";
  /**
   *
   */
  String HIERARCHY_JOINT_SYMBOL = "-";
  /**
   * 默认数据版本
   */
  long DEFAULT_VERSION_ID = 0L;
  /**
   * 默认数据版本名称
   */
  String DEFAULT_VERSION_NAME = "default";
  /**
   * 空应用字符串
   */
  String NULL_VALUE = "null";

}
