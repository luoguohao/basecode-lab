package com.luogh.learning.lab.lucene;

import com.luogh.learing.lab.lucene.filter.SpecialsAccessor;

public class TestSpecialsAccessor implements SpecialsAccessor {

  private String[] isbns;

  public TestSpecialsAccessor(String[] isbns) {
    this.isbns = isbns;
  }

  public String[] isbns() {
    return isbns;
  }
}

