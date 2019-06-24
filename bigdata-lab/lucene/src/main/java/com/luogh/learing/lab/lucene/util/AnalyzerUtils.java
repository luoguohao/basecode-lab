package com.luogh.learing.lab.lucene.util;

import java.io.IOException;
import java.io.StringReader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.junit.Assert;

public class AnalyzerUtils {

  public static void displayTokens(Analyzer analyzer, String text) throws IOException {
    displayTokens(analyzer.tokenStream("contents", new StringReader(text)));
  }

  public static void displayTokens(TokenStream stream) throws IOException {
    TermAttribute term = stream.addAttribute(TermAttribute.class);
    while (stream.incrementToken()) {
      System.out.print("[" + term.term() + "] ");
    }
  }

  public static void displayTokensWithFullDetails(Analyzer analyzer, String text)
      throws IOException {
    TokenStream stream = analyzer.tokenStream("contents", new StringReader(text));
    TermAttribute term = stream.addAttribute(TermAttribute.class);
    PositionIncrementAttribute posIncr = stream.addAttribute(PositionIncrementAttribute.class);
    OffsetAttribute offset = stream.addAttribute(OffsetAttribute.class);
    TypeAttribute type = stream.addAttribute(TypeAttribute.class);
    int position = 0;
    while (stream.incrementToken()) {
      int increment = posIncr.getPositionIncrement();
      if (increment > 0) {
        position = position + increment;
        System.out.println();
        System.out.print(position + ": ");
      }
      System.out.print(
          "[" + term.term() + ":" + offset.startOffset() + "->" + offset.endOffset() + ":" + type
              .type() + "] ");
    }
    System.out.println();
  }


  public static void assertAnalyzesTo(Analyzer analyzer, String input, String[] output)
      throws Exception {
    TokenStream stream = analyzer.tokenStream("field", new StringReader(input));
    TermAttribute termAttr = stream.addAttribute(TermAttribute.class);
    for (String expected : output) {
      Assert.assertTrue(stream.incrementToken());
      Assert.assertEquals(expected, termAttr.term());
    }
    Assert.assertFalse(stream.incrementToken());
    stream.close();
  }


}
