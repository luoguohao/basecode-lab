package com.luogh.learning.lab.lucene;

import com.luogh.learing.lab.lucene.SynonymAnalyzer;
import com.luogh.learing.lab.lucene.SynonymEngine;
import junit.framework.TestCase;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

public class MultiPhraseQueryTest extends TestCase {

  private IndexSearcher searcher;

  protected void setUp() throws Exception {
    Directory directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(),
        IndexWriter.MaxFieldLength.UNLIMITED);
    Document doc1 = new Document();
    doc1.add(new Field("field", "the quick brown fox jumped over the lazy dog", Field.Store.YES,
        Field.Index.ANALYZED));
    writer.addDocument(doc1);
    Document doc2 = new Document();
    doc2.add(new Field("field", "the fast fox hopped over the hound", Field.Store.YES,
        Field.Index.ANALYZED));
    writer.addDocument(doc2);
    writer.close();
    searcher = new IndexSearcher(directory);
  }

  /**
   * Using MultiPhraseQuery to match more than one term at each position
   */
  public void testBasic() throws Exception {
    MultiPhraseQuery query = new MultiPhraseQuery();
    query.add(new Term[]{new Term("field", "quick"), new Term("field", "fast")});
    query.add(new Term("field", "fox"));
    System.out.println(query);
    TopDocs hits = searcher.search(query, 10);
    assertEquals("fast fox match", 1, hits.totalHits);
    query.setSlop(1);
    hits = searcher.search(query, 10);
    assertEquals("both match", 2, hits.totalHits);
  }

  public void testAgainstOR() throws Exception {
    PhraseQuery quickFox = new PhraseQuery();
    quickFox.setSlop(1);
    quickFox.add(new Term("field", "quick"));
    quickFox.add(new Term("field", "fox"));
    PhraseQuery fastFox = new PhraseQuery();
    fastFox.add(new Term("field", "fast"));
    fastFox.add(new Term("field", "fox"));
    BooleanQuery query = new BooleanQuery();
    query.add(quickFox, BooleanClause.Occur.SHOULD);
    query.add(fastFox, BooleanClause.Occur.SHOULD);
    TopDocs hits = searcher.search(query, 10);
    assertEquals(2, hits.totalHits);
  }

  public void testQueryParser() throws Exception {
    SynonymEngine engine = new SynonymEngine() {
      public String[] getSynonyms(String s) {
        if (s.equals("quick")) {
          return new String[]{"fast"};
        } else {
          return null;
        }
      }
    };
    Query q = new QueryParser(Version.LUCENE_30, "field", new SynonymAnalyzer(engine))
        .parse("\"quick fox\"");
    assertEquals("analyzed", "field:\"(quick fast) fox\"", q.toString());
    assertTrue("parsed as MultiPhraseQuery", q instanceof MultiPhraseQuery);
  }


}
