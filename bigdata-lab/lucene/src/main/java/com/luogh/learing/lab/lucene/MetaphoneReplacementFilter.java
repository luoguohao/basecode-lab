package com.luogh.learing.lab.lucene;

import java.io.IOException;
import org.apache.commons.codec.language.Metaphone;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

public class MetaphoneReplacementFilter extends TokenFilter {

  public static final String METAPHONE = "metaphone";
  private Metaphone metaphoner = new Metaphone(); // 根据每个英文的发音，计算对应的散列，发音相近的单词，散列值相同
  private TermAttribute termAttr;
  private TypeAttribute typeAttr;

  public MetaphoneReplacementFilter(TokenStream input) {
    super(input);
    termAttr = addAttribute(TermAttribute.class);
    typeAttr = addAttribute(TypeAttribute.class);
  }

  public final boolean incrementToken() throws IOException {
    if (!input.incrementToken()) {
      return false;
    }
    String encoded;
    encoded = metaphoner.encode(termAttr.term());
    termAttr.setTermBuffer(encoded);
    typeAttr.setType(METAPHONE);
    return true;
  }
}

