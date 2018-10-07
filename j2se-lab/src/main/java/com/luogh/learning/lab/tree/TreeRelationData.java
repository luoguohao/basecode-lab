package com.luogh.learning.lab.tree;

import java.util.Map;
import lombok.Data;

/**
 * 生成树所需要的数据结构
 */
@Data
public class TreeRelationData {

  private String id;
  private Integer idType;
  private String parentId;
  private Integer parentIdType;
  private String text;
  private String hierarchyId;
  private Map<String, Object> additionProperties;
}
