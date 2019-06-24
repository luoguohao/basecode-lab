package com.luogh.learing.lab.lucene;


import com.google.common.base.Stopwatch;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class Indexer {

  private IndexWriter writer;

  public Indexer(String indexDir) throws Exception {
    Directory dir = FSDirectory.open(new File(indexDir));
    writer = new IndexWriter(dir, new StandardAnalyzer(Version.LUCENE_30), true,
        MaxFieldLength.UNLIMITED);
  }

  public void close() throws Exception {
    writer.close();
  }

  public int index(String dataDir, FileFilter filter)
      throws Exception {
    File[] files = new File(dataDir).listFiles();
    for (File f : files) {
      if (!f.isDirectory() &&
          !f.isHidden() &&
          f.exists() &&
          f.canRead() &&
          (filter == null || filter.accept(f))) {
        indexFile(f);
      }
    }
    return writer.numDocs();
  }

  private static class TextFilesFilter implements FileFilter {

    public boolean accept(File path) {
      return path.getName().toLowerCase()
          .endsWith(".java");
    }
  }

  protected Document getDocument(File f) throws Exception {
    Document doc = new Document();
    doc.add(new Field("contents", new FileReader(f)));
    doc.add(new Field("filename", f.getName(),
        Field.Store.YES, Field.Index.NOT_ANALYZED));
    doc.add(new Field("fullpath", f.getCanonicalPath(),
        Field.Store.YES, Field.Index.NOT_ANALYZED));
    return doc;
  }

  private void indexFile(File f) throws Exception {
    System.out.println("Indexing " + f.getCanonicalPath());
    Document doc = getDocument(f);
    writer.addDocument(doc);
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      throw new IllegalArgumentException(
          "Usage: java " + Indexer.class.getName() + "<index dir> <data dir>");
    }

    String indexDir = args[0];
    String dataDir = args[1];

    Stopwatch stopWatch = Stopwatch.createStarted();
    Indexer indexer = new Indexer(indexDir);
    int numIndexed;
    try {
      numIndexed = indexer.index(dataDir, new TextFilesFilter());
    } finally {
      indexer.close();
    }

    long cost = stopWatch.elapsed(TimeUnit.MILLISECONDS);
    System.out.println("Indexing " + numIndexed + " files took " + cost + " milliseconds");
  }
}
