package com.luogh.learning.lab.tree;

import com.google.common.base.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class Tuple<K, V> {
  protected final K key;
  protected final V value;

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Tuple)) {
      return false;
    }
    Tuple<?, ?> tuple = (Tuple<?, ?>) o;
    return Objects.equal(key, tuple.key) &&
        Objects.equal(value, tuple.value);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(key, value);
  }
}

