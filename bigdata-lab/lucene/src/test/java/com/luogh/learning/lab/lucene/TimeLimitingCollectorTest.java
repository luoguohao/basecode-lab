package com.luogh.learning.lab.lucene;

import com.luogh.learning.lab.lucene.common.TestUtil;
import junit.framework.TestCase;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TimeLimitingCollector;
import org.apache.lucene.search.TimeLimitingCollector.TimeExceededException;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;

public class TimeLimitingCollectorTest extends TestCase {

  public void testTimeLimitingCollector() throws Exception {
    Directory dir = TestUtil.getBookIndexDirectory();
    IndexSearcher searcher = new IndexSearcher(dir);
    Query q = new MatchAllDocsQuery();
    int numAllBooks = TestUtil.hitCount(searcher, q);
    TopScoreDocCollector topDocs = TopScoreDocCollector.create(10, false);
    Collector collector = new TimeLimitingCollector(topDocs, 1000);
    try {
      searcher.search(q, collector); // 使用查询时间限制的Collector实现，可以中断慢查询
      assertEquals(numAllBooks, topDocs.getTotalHits());
    } catch (TimeExceededException tee) {
      System.out.println("Too much time taken.");
    }
    searcher.close();
    dir.close();
  }
}

