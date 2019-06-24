package com.luogh.learning.lab.lucene;

import com.luogh.learing.lab.lucene.MetaphoneReplacementAnalyzer;
import com.luogh.learning.lab.lucene.common.TestUtil;
import junit.framework.TestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

public class BasicSearchingTest extends TestCase {

  private Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_30);

  public void testTerm() throws Exception {
    Directory dir = TestUtil.getBookIndexDirectory();
    IndexSearcher searcher = new IndexSearcher(dir);
    Term t = new Term("subject", "ant");
    Query query = new TermQuery(t);
    TopDocs docs = searcher.search(query, 10);
    assertEquals("Ant in Action", 1, docs.totalHits);
    t = new Term("subject", "junit");
    docs = searcher.search(new TermQuery(t), 10);
    assertEquals("Ant in Action, " + "JUnit in Action, Second Edition", 2, docs.totalHits);
    searcher.close();
    dir.close();
  }

  public void testQueryParser() throws Exception {
    Directory dir = TestUtil.getBookIndexDirectory();
    IndexSearcher searcher = new IndexSearcher(dir);
    QueryParser parser = new QueryParser(Version.LUCENE_30, "contents", new SimpleAnalyzer());
    Query query = parser.parse("+JUNIT +ANT -MOCK");
    TopDocs docs = searcher.search(query, 10);
    assertEquals(1, docs.totalHits);
    Document d = searcher.doc(docs.scoreDocs[0].doc);
    assertEquals("Ant in Action", d.get("title"));
    query = parser.parse("mock OR junit");
    docs = searcher.search(query, 10);
    assertEquals("Ant in Action, " + "JUnit in Action, Second Edition", 2, docs.totalHits);
    searcher.close();
    dir.close();
  }

  public void testNearRealTime() throws Exception {
    Directory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(Version.LUCENE_30),
        IndexWriter.MaxFieldLength.UNLIMITED);
    for (int i = 0; i < 10; i++) {
      Document doc = new Document();
      doc.add(new Field("id", "" + i, Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
      doc.add(new Field("text", "aaa", Field.Store.NO, Field.Index.ANALYZED));
      writer.addDocument(doc);
    }

    IndexReader reader = writer.getReader();
    IndexSearcher searcher = new IndexSearcher(reader);
    Query query = new TermQuery(new Term("text", "aaa"));
    TopDocs docs = searcher.search(query, 1);
    assertEquals(10, docs.totalHits);

    writer.deleteDocuments(new Term("id", "7"));
    Document doc = new Document();
    doc.add(new Field("id", "11", Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
    doc.add(new Field("text", "bbb", Field.Store.NO, Field.Index.ANALYZED));
    writer.addDocument(doc);

    IndexReader newReader = reader.reopen();
    assertFalse(reader == newReader);
    reader.close();
    searcher = new IndexSearcher(newReader);
    TopDocs hits = searcher.search(query, 10);
    assertEquals(9, hits.totalHits);

    query = new TermQuery(new Term("text", "bbb"));
    hits = searcher.search(query, 1);
    assertEquals(1, hits.totalHits);
    newReader.close();
    writer.close();

  }

  public void testKeyword() throws Exception {
    Directory dir = TestUtil.getBookIndexDirectory();
    IndexSearcher searcher = new IndexSearcher(dir);
    Term t = new Term("isbn", "9781935182023");
    Query query = new TermQuery(t);
    TopDocs docs = searcher.search(query, 10);
    assertEquals("JUnit in Action, Second Edition", 1, docs.totalHits);
    searcher.close();
    dir.close();
  }

  public void testTermRangeQuery() throws Exception {
    Directory dir = TestUtil.getBookIndexDirectory();
    IndexSearcher searcher = new IndexSearcher(dir);
    TermRangeQuery query = new TermRangeQuery("title2", "d", "j", true, true);
    TopDocs matches = searcher.search(query, 100);
    assertEquals(3, matches.totalHits);
    searcher.close();
    dir.close();
  }

  public void testInclusive() throws Exception {
    Directory dir = TestUtil.getBookIndexDirectory();
    IndexSearcher searcher = new IndexSearcher(
        dir); // pub date of TTC was September 2006
    NumericRangeQuery query = NumericRangeQuery.newIntRange("pubmonth", 200605, 200609, true, true);
    TopDocs matches = searcher.search(query, 10);
    assertEquals(1, matches.totalHits);
    searcher.close();
    dir.close();
  }

  public void testExclusive() throws Exception {
    Directory dir = TestUtil.getBookIndexDirectory();
    IndexSearcher searcher = new IndexSearcher(
        dir); // pub date of TTC was September 2006
    NumericRangeQuery query = NumericRangeQuery
        .newIntRange("pubmonth", 200605, 200609, false, false);
    TopDocs matches = searcher.search(query, 10);
    assertEquals(0, matches.totalHits);
    searcher.close();
    dir.close();
  }

  public void testPrefix() throws Exception {
    Directory dir = TestUtil.getBookIndexDirectory();
    IndexSearcher searcher = new IndexSearcher(dir);
    Term term = new Term("category", "/technology/computers/programming");
    PrefixQuery query = new PrefixQuery(term);
    TopDocs matches = searcher.search(query, 10);
    int programmingAndBelow = matches.totalHits;
    matches = searcher.search(new TermQuery(term), 10);
    int justProgramming = matches.totalHits;
    assertTrue(programmingAndBelow > justProgramming);
    searcher.close();
    dir.close();
  }

  public void testAnd() throws Exception {
    TermQuery searchingBooks = new TermQuery(new Term("subject", "search"));
    Query books2010 = NumericRangeQuery.newIntRange("pubmonth", 201001, 201012, true, true);
    BooleanQuery searchingBooks2010 = new BooleanQuery();
    searchingBooks2010.add(searchingBooks, BooleanClause.Occur.MUST);
    searchingBooks2010.add(books2010, BooleanClause.Occur.MUST);
    Directory dir = TestUtil.getBookIndexDirectory();
    IndexSearcher searcher = new IndexSearcher(dir);
    TopDocs matches = searcher.search(searchingBooks2010, 10);
    assertTrue(TestUtil.hitsIncludeTitle(searcher, matches, "Lucene in Action, Second Edition"));
    searcher.close();
    dir.close();
  }

  public void testOr() throws Exception {
    TermQuery methodologyBooks = new TermQuery(
        new Term("category", "/technology/computers/programming/methodology"));
    TermQuery easternPhilosophyBooks = new TermQuery(new Term("category", "/philosophy/eastern"));
    BooleanQuery enlightenmentBooks = new BooleanQuery();
    enlightenmentBooks.add(methodologyBooks, BooleanClause.Occur.SHOULD);
    enlightenmentBooks.add(easternPhilosophyBooks, BooleanClause.Occur.SHOULD);
    Directory dir = TestUtil.getBookIndexDirectory();
    IndexSearcher searcher = new IndexSearcher(dir);
    TopDocs matches = searcher.search(enlightenmentBooks, 10);
    System.out.println("or = " + enlightenmentBooks);
    assertTrue(TestUtil.hitsIncludeTitle(searcher, matches, "Extreme Programming Explained"));
    assertTrue(TestUtil.hitsIncludeTitle(searcher, matches, "Tao Te Ching \u9053\u5FB7\u7D93"));
    searcher.close();
    dir.close();
  }

  private void indexSingleFieldDocs(Directory directory, Field[] fields) throws Exception {
    IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(),
        IndexWriter.MaxFieldLength.UNLIMITED);
    for (Field f : fields) {
      Document doc = new Document();
      doc.add(f);
      writer.addDocument(doc);
    }
    writer.optimize();
    writer.close();
  }

  public void testWildcard() throws Exception {
    Directory directory = new RAMDirectory();

    indexSingleFieldDocs(directory,
        new Field[]{new Field("contents", "wild", Field.Store.YES, Field.Index.ANALYZED),
            new Field("contents", "child", Field.Store.YES, Field.Index.ANALYZED),
            new Field("contents", "mild", Field.Store.YES, Field.Index.ANALYZED),
            new Field("contents", "mildew", Field.Store.YES, Field.Index.ANALYZED)});

    IndexSearcher searcher = new IndexSearcher(directory);
    Query query = new WildcardQuery(new Term("contents", "?ild*"));
    TopDocs matches = searcher.search(query, 10);

    assertEquals("child no match", 3, matches.totalHits);
    assertEquals("score the same", matches.scoreDocs[0].score, matches.scoreDocs[1].score, 0.0);
    assertEquals("score the same", matches.scoreDocs[1].score, matches.scoreDocs[2].score, 0.0);

    searcher.close();
  }

  public void testFuzzy() throws Exception {
    Directory directory = new RAMDirectory();
    indexSingleFieldDocs(directory,
        new Field[]{new Field("contents", "fuzzy", Field.Store.YES, Field.Index.ANALYZED),
            new Field("contents", "wuzzy", Field.Store.YES, Field.Index.ANALYZED)}
    );

    IndexSearcher searcher = new IndexSearcher(directory);
    Query query = new FuzzyQuery(new Term("contents", "wuzza"));
    TopDocs matches = searcher.search(query, 10);

    assertEquals("both close enough", 2, matches.totalHits);
    assertTrue("wuzzy closer than fuzzy", matches.scoreDocs[0].score != matches.scoreDocs[1].score);

    Document doc = searcher.doc(matches.scoreDocs[0].doc);
    assertEquals("wuzza bear", "wuzzy", doc.get("contents"));
    searcher.close();
  }

  public void testToString() throws Exception {
    BooleanQuery query = new BooleanQuery();
    query.add(new FuzzyQuery(new Term("field", "kountry")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term("title", "western")), BooleanClause.Occur.SHOULD);
    assertEquals("both kinds", "+kountry~0.5 title:western", query.toString("field"));
  }

  public void testTermQuery() throws Exception {
    QueryParser parser = new QueryParser(Version.LUCENE_30, "subject",
        new StandardAnalyzer(Version.LUCENE_30));
    Query query = parser.parse("computers");
    assertEquals("subject:computers", query.toString());
  }

  public void testTermRangeQueryParser() throws Exception {
    Query query = new QueryParser(Version.LUCENE_30, "subject",
        new StandardAnalyzer(Version.LUCENE_30)).parse("title2:[Q TO V]"); // 闭区间
    assertTrue(query instanceof TermRangeQuery);

    IndexSearcher searcher = new IndexSearcher(TestUtil.getBookIndexDirectory());

    TopDocs matches = searcher.search(query, 10);
    assertTrue(TestUtil.hitsIncludeTitle(searcher, matches, "Tapestry in Action"));

    query = new QueryParser(Version.LUCENE_30, "subject", new StandardAnalyzer(Version.LUCENE_30))
        .parse("title2:{Q TO \"Tapestry in Action\" }"); // 开区间
    matches = searcher.search(query, 10);
    assertFalse(TestUtil.hitsIncludeTitle(searcher, matches, "Tapestry in Action"));
  }

  public void testLowercasing() throws Exception {
    Query q = new QueryParser(Version.LUCENE_30, "field", new StandardAnalyzer(Version.LUCENE_30))
        .parse("PrefixQuery*");
    assertEquals("lowercased", "prefixquery*", q.toString("field"));
    QueryParser qp = new QueryParser(Version.LUCENE_30, "field",
        new StandardAnalyzer(Version.LUCENE_30));
    qp.setLowercaseExpandedTerms(false);
    q = qp.parse("PrefixQuery*");
    assertEquals("not lowercased", "PrefixQuery*", q.toString("field"));
  }

  public void testPhraseQuery() throws Exception {
    Query q = new QueryParser(Version.LUCENE_30, "field",
        new StandardAnalyzer(Version.LUCENE_30))
        .parse("\"This is Some Phrase*\""); // 使用双引号扩起来的，表示词组，会通过分词器进行分词
    // // 此处的问号占位符，因为This和is被分词器认为是停用词，被过滤了，并且默认的词组各个词之间的slot为0
    assertEquals("analyzed", "\"? ? some phrase\"", q.toString("field"));

    q = new QueryParser(Version.LUCENE_30, "field", new StandardAnalyzer(Version.LUCENE_30))
        .parse("\"term\"");
    assertTrue("reduced to TermQuery", q instanceof TermQuery);
  }

  public void testSlop() throws Exception {
    Query q = new QueryParser(Version.LUCENE_30, "field", analyzer).parse("\"exact phrase\"");
    assertEquals("zero slop", "\"exact phrase\"", q.toString("field"));
    QueryParser qp = new QueryParser(Version.LUCENE_30, "field", analyzer);
    qp.setPhraseSlop(5); // 设置slop大小，默认为0
    q = qp.parse("\"sloppy phrase\"");
    assertEquals("sloppy, implicitly", "\"sloppy phrase\"~5", q.toString("field"));
  }

  public void testFuzzyQuery() throws Exception {
    QueryParser parser = new QueryParser(Version.LUCENE_30, "subject", analyzer);
    Query query = parser.parse("kountry~"); // 在phrase query中 ～ 用于指定slop大小，在模糊查询中，表示编辑距离的阈值
    System.out.println("fuzzy: " + query);
    query = parser.parse("kountry~0.7");
    System.out.println("fuzzy 2: " + query);
  }

  public void testGrouping() throws Exception {
    Query query = new QueryParser(Version.LUCENE_30, "subject", analyzer)
        .parse("(agile OR extreme) AND methodology");

    IndexSearcher searcher = new IndexSearcher(TestUtil.getBookIndexDirectory());

    TopDocs matches = searcher.search(query, 10);
    assertTrue(TestUtil.hitsIncludeTitle(searcher, matches, "Extreme Programming Explained"));
    assertTrue(TestUtil.hitsIncludeTitle(searcher, matches, "The Pragmatic Programmer"));
  }

  public void testKoolKat() throws Exception {
    RAMDirectory directory = new RAMDirectory();
    Analyzer analyzer = new MetaphoneReplacementAnalyzer();
    IndexWriter writer = new IndexWriter(directory, analyzer, true,
        IndexWriter.MaxFieldLength.UNLIMITED);
    Document doc = new Document();
    doc.add(new Field("contents", "cool cat", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);
    writer.close();
    IndexSearcher searcher = new IndexSearcher(directory);
    Query query = new QueryParser(Version.LUCENE_30, "contents", analyzer).parse("kool kat");
    TopDocs hits = searcher.search(query, 1);
    assertEquals(1, hits.totalHits);
    int docID = hits.scoreDocs[0].doc;
    doc = searcher.doc(docID);
    assertEquals("cool cat", doc.get("contents"));
    searcher.close();
  }

}

