package com.luogh.learning.lab.lucene;

import com.luogh.learing.lab.lucene.analyzer.MetaphoneReplacementAnalyzer;
import com.luogh.learing.lab.lucene.analyzer.StopAnalyzer2;
import com.luogh.learing.lab.lucene.analyzer.StopAnalyzerFlawed;
import com.luogh.learing.lab.lucene.util.AnalyzerUtils;
import java.io.IOException;
import junit.framework.TestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

public class AnalyzerTest extends TestCase {

  private static final String[] examples = {"The quick brown fox jumped over the lazy dog",
      "XY&Z Corporation - xyz@example.com"};
  private static final Analyzer[] analyzers = new Analyzer[]{new WhitespaceAnalyzer(),
      new SimpleAnalyzer(), new StopAnalyzer(Version.LUCENE_30),
      new StandardAnalyzer(Version.LUCENE_30)};

  public void testAnalyze() throws Exception {
    for (String text : examples) {
      analyze(text);
    }
  }

  public void testAnalyzeDetailInfo() throws Exception {
    for (String text : examples) {
      for (Analyzer analyzer : analyzers) {
        System.out.println(
            "Using analyzer " + analyzer.getClass().getSimpleName() + " to analyze " + text);
        AnalyzerUtils.displayTokensWithFullDetails(analyzer, text);
      }
    }
  }


  public void testStopAnalyzer2() throws Exception {
    AnalyzerUtils.assertAnalyzesTo(new StopAnalyzer2(), "The quick brown...",
        new String[]{"quick", "brown"});
  }

  public void testStopAnalyzerFlawed() throws Exception {
    AnalyzerUtils.assertAnalyzesTo(new StopAnalyzerFlawed(), "The quick brown...",
        new String[]{"the", "quick", "brown"});
  }

  public void testMetaPhoneAnalyzer() throws Exception {
    MetaphoneReplacementAnalyzer analyzer = new MetaphoneReplacementAnalyzer();
    AnalyzerUtils.displayTokens(analyzer, "The quick brown fox jumped over the lazy dog");
    System.out.println();
    AnalyzerUtils.displayTokens(analyzer, "Tha quik brown phox jumpd ovvar tha lazi dag");
  }


  public void testPerFieldAnalyzer() throws Exception {
    PerFieldAnalyzerWrapper analyzer = new PerFieldAnalyzerWrapper(new SimpleAnalyzer());
    analyzer.addAnalyzer("partnum", new KeywordAnalyzer());
    Query query = new QueryParser(Version.LUCENE_30, "description", analyzer)
        .parse("partnum:Q36 AND SPACE");
    assertEquals("Q36 kept as-is", "+partnum:Q36 +space", query.toString("description"));
  }


  private void analyze(String text) throws IOException {
    System.out.println("Analyzing \"" + text + "\"");
    for (Analyzer analyzer : analyzers) {
      String name = analyzer.getClass().getSimpleName();
      System.out.println(" " + name + ":");
      System.out.print(" ");
      AnalyzerUtils.displayTokens(analyzer, text);
      System.out.println("\n");
    }
  }

}

