package com.luogh.learning.lab.lucene;

import com.luogh.learning.lab.lucene.common.TestUtil;
import junit.framework.TestCase;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldCacheRangeFilter;
import org.apache.lucene.search.FieldCacheTermsFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.search.PrefixFilter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.SpanQueryFilter;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeFilter;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;

public class FilterTest extends TestCase {

  private Query allBooks;
  private IndexSearcher searcher;
  private Directory dir;

  protected void setUp() throws Exception {
    allBooks = new MatchAllDocsQuery();
    dir = TestUtil.getBookIndexDirectory();
    searcher = new IndexSearcher(dir);
  }

  protected void tearDown() throws Exception {
    searcher.close();
    dir.close();
  }

  public void testTermRangeFilter() throws Exception {
    Filter filter = new TermRangeFilter("title2", "d", "j", true, true);
    assertEquals(3, TestUtil.hitCount(searcher, allBooks, filter));

    DocIdSet idSet = filter.getDocIdSet(IndexReader.open(dir));
    DocIdSetIterator iterator = idSet.iterator();
    int nexDocId;
    while ((nexDocId = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      System.out.println("docID:" + nexDocId);
    }

    // filter = new TermRangeFilter("modified", null, jan31, false, true); // 范围：(-inf, jan31]
    // filter = new TermRangeFilter("modified", jan1, null, true, false);  // 范围：[jan1, +inf)
    // 同以下方式：
    // filter = TermRangeFilter.Less("modified", jan31);
    // filter = TermRangeFilter.More("modified", jan1);
  }

  public void testNumericDateFilter() throws Exception {
    Filter filter = NumericRangeFilter.newIntRange("pubmonth", 201001, 201006, true, true);
    assertEquals(2, TestUtil.hitCount(searcher, allBooks, filter));
  }

  public void testFieldCacheFilter() throws Exception {
    Filter filter = FieldCacheRangeFilter.newStringRange("title2", "d", "j", true, true);
    assertEquals(3, TestUtil.hitCount(searcher, allBooks, filter));

    filter = FieldCacheRangeFilter.newIntRange("pubmonth", 201001, 201006, true, true);
    assertEquals(2, TestUtil.hitCount(searcher, allBooks, filter));
  }

  public void testFieldCacheTermsFilter() throws Exception {
    Filter filter = new FieldCacheTermsFilter("category",
        "/health/alternative/chinese", "/technology/computers/ai",
        "/technology/computers/programming");
    assertEquals("expected 7 hits", 7, TestUtil.hitCount(searcher, allBooks, filter));
  }

  /**
   * query转filter
   */
  public void testQueryWrapperFilter() throws Exception {
    TermQuery categoryQuery = new TermQuery(new Term("category", "/philosophy/eastern"));
    Filter categoryFilter = new QueryWrapperFilter(categoryQuery);
    System.out.println(categoryFilter.toString());
    assertEquals("only tao te ching", 1, TestUtil.hitCount(searcher, allBooks, categoryFilter));
  }


  /**
   * Span Query转Span Filter保留span信息
   */
  public void testSpanQueryFilter() throws Exception {
    SpanQuery categoryQuery = new SpanTermQuery(new Term("category", "/philosophy/eastern"));
    Filter categoryFilter = new SpanQueryFilter(categoryQuery);
    assertEquals("only tao te ching", 1, TestUtil.hitCount(searcher, allBooks, categoryFilter));
  }

  public void testFilterAlternative() throws Exception {
    TermQuery categoryQuery = new TermQuery(new Term("category", "/philosophy/eastern"));
    BooleanQuery constrainedQuery = new BooleanQuery();
    constrainedQuery.add(allBooks, BooleanClause.Occur.MUST);
    constrainedQuery.add(categoryQuery, BooleanClause.Occur.MUST);
    assertEquals("only tao te ching", 1, TestUtil.hitCount(searcher, allBooks, new QueryWrapperFilter(constrainedQuery)));
  }

  public void testPrefixFilter() throws Exception {
    Filter prefixFilter = new PrefixFilter(new Term("category", "/technology/computers"));
    assertEquals("only /technology/computers/* books",
        8, TestUtil.hitCount(searcher, allBooks, prefixFilter));
  }

  public void testCachingWrapper() throws Exception {
    Filter filter = new TermRangeFilter("title2", "d", "j", true, true);
    CachingWrapperFilter cachingFilter = new CachingWrapperFilter(filter);
    assertEquals(3, TestUtil.hitCount(searcher, allBooks, cachingFilter));
  }

  public void testConvertFilterToQuery() throws Exception {
    Filter filter = new TermRangeFilter("title2", "d", "j", true, true);
    ConstantScoreQuery query = new ConstantScoreQuery(filter);
    assertEquals(3, TestUtil.hitCount(searcher, query));
  }


}

