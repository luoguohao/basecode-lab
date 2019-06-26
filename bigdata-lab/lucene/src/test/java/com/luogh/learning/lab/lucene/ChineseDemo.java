package com.luogh.learning.lab.lucene;

import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Frame;
import java.awt.Label;
import java.io.IOException;
import java.io.StringReader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.analysis.cn.ChineseAnalyzer;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.util.Version;

public class ChineseDemo {

  private static String[] strings = {"道德經"};
  private static Analyzer[] analyzers = {new SimpleAnalyzer(),
      new StandardAnalyzer(Version.LUCENE_30), new ChineseAnalyzer(),
      new CJKAnalyzer(Version.LUCENE_30), new SmartChineseAnalyzer(Version.LUCENE_30)};

  public static void main(String args[]) throws Exception {
    for (String string : strings) {
      for (Analyzer analyzer : analyzers) {
        analyze(string, analyzer);
      }
    }
  }

  private static void analyze(String string, Analyzer analyzer) throws IOException {
    StringBuffer buffer = new StringBuffer();
    TokenStream stream = analyzer.tokenStream("contents", new StringReader(string));
    TermAttribute term = stream.addAttribute(TermAttribute.class);
    while (stream.incrementToken()) {
      buffer.append("[");
      buffer.append(term.term());
    }

    String output = buffer.toString();
    Frame f = new Frame();
    f.setTitle(analyzer.getClass().getSimpleName() + " : " + string);
    f.setResizable(false);
    Font font = new Font(null, Font.PLAIN, 36);
    int width = getWidth(f.getFontMetrics(font), output);
    f.setSize((width < 250) ? 250 : width + 50, 75);
    Label label = new Label(output);
    label.setSize(width, 75);
    label.setAlignment(Label.CENTER);

    label.setFont(font);
    f.add(label);
    f.setVisible(true);
  }

  private static int getWidth(FontMetrics metrics, String s) {
    int size = 0;
    int length = s.length();
    for (int i = 0; i < length; i++) {
      size += metrics.charWidth(s.charAt(i));
    }
    return size;
  }
}


