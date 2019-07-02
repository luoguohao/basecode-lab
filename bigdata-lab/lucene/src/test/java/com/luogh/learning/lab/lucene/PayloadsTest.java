package com.luogh.learning.lab.lucene;

import com.luogh.learing.lab.lucene.BoostingSimilarity;
import com.luogh.learing.lab.lucene.analyzer.BulletinPayloadsAnalyzer;
import com.luogh.learning.lab.lucene.common.TestUtil;
import java.io.IOException;
import junit.framework.TestCase;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.payloads.AveragePayloadFunction;
import org.apache.lucene.search.payloads.PayloadTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

public class PayloadsTest extends TestCase {

  Directory dir;
  IndexWriter writer;
  BulletinPayloadsAnalyzer analyzer;

  protected void setUp() throws Exception {
    super.setUp();
    dir = new RAMDirectory();
    analyzer = new BulletinPayloadsAnalyzer(5.0F);
    writer = new IndexWriter(dir, analyzer, IndexWriter.MaxFieldLength.UNLIMITED);
  }

  protected void tearDown() throws Exception {
    super.tearDown();
    writer.close();
  }

  void addDoc(String title, String contents) throws IOException {
    Document doc = new Document();
    doc.add(new Field("title", title, Field.Store.YES, Field.Index.NO));
    doc.add(new Field("contents", contents, Field.Store.NO, Field.Index.ANALYZED));

    analyzer.setBulletin(contents.startsWith("Bulletin:"));
    writer.addDocument(doc);
  }

  public void testPayloadTermQuery() throws Throwable {
    addDoc("Hurricane warning",
        "Bulletin: A hurricane warning was issued " + "at 6 AM for the outer great banks");
    addDoc("Warning label maker", "The warning label maker is a delightful toy for "
        + "your precocious seven year old's warning needs");
    addDoc("Tornado warning",
        "Bulletin: There is a tornado warning for " + "Worcester county until 6 PM today");

    IndexReader r = writer.getReader();
    writer.close();

    IndexSearcher searcher = new IndexSearcher(r);
    searcher.setSimilarity(new BoostingSimilarity());

    Term warning = new Term("contents", "warning");
    Query query1 = new TermQuery(warning);
    System.out.println("\nTermQuery results:");

    TopDocs hits = searcher.search(query1, 10);
    TestUtil.dumpHits(searcher, hits);

    assertEquals("Warning label maker", searcher.doc(hits.scoreDocs[0].doc).get("title"));

    Query query2 = new PayloadTermQuery(warning, new AveragePayloadFunction());
    System.out.println("\nPayloadTermQuery results:");
    hits = searcher.search(query2, 10);
    TestUtil.dumpHits(searcher, hits);

    assertEquals("Warning label maker", searcher.doc(hits.scoreDocs[2].doc).get("title"));

    r.close();
    searcher.close();
  }
}

