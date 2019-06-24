package com.luogh.learning.lab.lucene;

import java.io.File;
import java.io.IOException;
import junit.framework.TestCase;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;

public class LockTest extends TestCase {

  private Directory dir;

  protected void setUp() throws IOException {
    String indexDir =
        System.getProperty("java.io.tmpdir", "tmp") + System.getProperty("file.separator")
            + "index";
    dir = FSDirectory.open(new File(indexDir));
  }

  public void testWriteLock() throws IOException {
    IndexWriter writer1 = new IndexWriter(dir, new SimpleAnalyzer(),
        IndexWriter.MaxFieldLength.UNLIMITED);
    IndexWriter writer2 = null;
    try {
      writer2 = new IndexWriter(dir, new SimpleAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
      fail("We should never reach this point");
    } catch (LockObtainFailedException e) {
      e.printStackTrace();
    } finally {
      writer1.close();
      assertNull(writer2);
    }

  }
}
