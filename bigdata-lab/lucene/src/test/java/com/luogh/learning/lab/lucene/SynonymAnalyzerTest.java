package com.luogh.learning.lab.lucene;

import com.luogh.learing.lab.lucene.SynonymAnalyzer;
import com.luogh.learing.lab.lucene.TestSynonymEngine;
import com.luogh.learning.lab.lucene.common.TestUtil;
import java.io.StringReader;
import junit.framework.TestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

public class SynonymAnalyzerTest extends TestCase {

  private IndexSearcher searcher;
  private static SynonymAnalyzer synonymAnalyzer = new SynonymAnalyzer(new TestSynonymEngine());

  public void setUp() throws Exception {
    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory, synonymAnalyzer,
        IndexWriter.MaxFieldLength.UNLIMITED);
    Document doc = new Document();
    doc.add(new Field("content", "The quick brown fox jumps over the lazy dog", Field.Store.YES,
        Field.Index.ANALYZED));
    writer.addDocument(doc);
    writer.close();
    searcher = new IndexSearcher(directory);
  }

  public void tearDown() throws Exception {
    searcher.close();
  }

  public void testSearchByAPI() throws Exception {
    TermQuery tq = new TermQuery(new Term("content", "hops"));
    assertEquals(1, TestUtil.hitCount(searcher, tq));
    PhraseQuery pq = new PhraseQuery();
    pq.add(new Term("content", "fox"));
    pq.add(new Term("content", "hops"));
    assertEquals(1, TestUtil.hitCount(searcher, pq));
  }

  public void testJumps() throws Exception {
    TokenStream stream = synonymAnalyzer.tokenStream("contents", new StringReader("jumps"));
    TermAttribute term = stream.addAttribute(TermAttribute.class);
    PositionIncrementAttribute posIncr = stream.addAttribute(PositionIncrementAttribute.class);
    int i = 0;
    String[] expected = new String[]{"jumps", "hops", "leaps"};
    while (stream.incrementToken()) {
      assertEquals(expected[i], term.term());
      int expectedPos;
      if (i == 0) {
        expectedPos = 1;
      } else {
        expectedPos = 0;
      }
      assertEquals(expectedPos, posIncr.getPositionIncrement());
      i++;
    }
    assertEquals(3, i);
  }

  public void testWithQueryParser() throws Exception {
    Query query = new QueryParser(Version.LUCENE_30, "content", synonymAnalyzer)
        .parse("\"fox jumps\"");
    assertEquals(1, TestUtil.hitCount(searcher, query));
    System.out
        .println("With SynonymAnalyzer, \"fox jumps\" parses to " + query.toString("content"));
    query = new QueryParser(Version.LUCENE_30, "content", new StandardAnalyzer(Version.LUCENE_30))
        .parse("\"fox jumps\"");
    assertEquals(1, TestUtil.hitCount(searcher, query));
    System.out
        .println("With StandardAnalyzer, \"fox jumps\" parses to " + query.toString("content"));
  }

}