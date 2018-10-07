package com.luogh.learning.lab.tree;

import lombok.Getter;


@Getter
public class CTuple<K extends Comparable<K>, V extends Comparable<V>> extends Tuple<K, V> implements
    Comparable<CTuple<K, V>> {

  public CTuple(K key, V value) {
    super(key, value);
  }

  @Override
  public int compareTo(CTuple<K, V> o) { // sorted by value with reverse order
    if (o != null) {
      if (o.getValue() == null) {
        return this.getValue() == null ? 0 : -1;
      } else {
        if (this.getValue() == null) {
          return 1;
        } else {
          return -1 * this.getValue().compareTo(o.getValue()); // reverse order
        }
      }
    } else {
      return -1;
    }
  }
}
