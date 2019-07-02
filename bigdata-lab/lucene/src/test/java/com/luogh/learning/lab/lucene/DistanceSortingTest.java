package com.luogh.learning.lab.lucene;

import java.io.IOException;
import junit.framework.TestCase;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.RAMDirectory;

/**
 * 自定义sort
 */
public class DistanceSortingTest extends TestCase {

  private RAMDirectory directory;
  private IndexSearcher searcher;
  private Query query;

  protected void setUp() throws Exception {
    directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(),
        IndexWriter.MaxFieldLength.UNLIMITED);
    addPoint(writer, "El Charro", "restaurant", 1, 2);
    addPoint(writer, "Cafe Poca Cosa", "restaurant", 5, 9);
    addPoint(writer, "Los Betos", "restaurant", 9, 6);
    addPoint(writer, "Nico's Taco Shop", "restaurant", 3, 8);
    writer.close();
    searcher = new IndexSearcher(directory);
    query = new TermQuery(new Term("type", "restaurant"));
  }

  private void addPoint(IndexWriter writer, String name, String type, int x, int y)
      throws IOException {
    Document doc = new Document();
    doc.add(new Field("name", name, Field.Store.YES, Field.Index.NOT_ANALYZED));
    doc.add(new Field("type", type, Field.Store.YES, Field.Index.NOT_ANALYZED));
    doc.add(new Field("location", x + "," + y, Field.Store.YES, Field.Index.NOT_ANALYZED));
    writer.addDocument(doc);
  }

  public void testNearestRestaurantToHome() throws Exception {
    Sort sort = new Sort(new SortField("location", new DistanceComparatorSource(0, 0)));
    TopDocs hits = searcher.search(query, null, 10, sort);
    assertEquals("closest", "El Charro", searcher.doc(hits.scoreDocs[0].doc).get("name"));
    assertEquals("furthest", "Los Betos", searcher.doc(hits.scoreDocs[3].doc).get("name"));
  }

  public void testNeareastRestaurantToWork() throws Exception {
    Sort sort = new Sort(new SortField("location", new DistanceComparatorSource(10, 10)));
    TopFieldDocs docs = searcher.search(query, null, 3, sort);
    assertEquals(4, docs.totalHits);
    assertEquals(3, docs.scoreDocs.length);
    FieldDoc fieldDoc = (FieldDoc) docs.scoreDocs[0];
    assertEquals("(10,10) -> (9,6) = sqrt(17)", (float) Math.sqrt(17), fieldDoc.fields[0]);
    Document document = searcher.doc(fieldDoc.doc);
    assertEquals("Los Betos", document.get("name"));
  }


  public static class DistanceComparatorSource extends FieldComparatorSource {

    private int x;
    private int y;

    public DistanceComparatorSource(int x, int y) {
      this.x = x;
      this.y = y;
    }

    public FieldComparator newComparator(java.lang.String fieldName, int numHits, int sortPos,
        boolean reversed) throws IOException {
      return new DistanceScoreDocLookupComparator(fieldName, numHits);
    }

    private class DistanceScoreDocLookupComparator extends FieldComparator {

      private int[] xDoc, yDoc;
      private float[] values;
      private float bottom;
      String fieldName;

      public DistanceScoreDocLookupComparator(String fieldName, int numHits) throws IOException {
        values = new float[numHits];
        this.fieldName = fieldName;
      }

      public void setNextReader(IndexReader reader, int docBase) throws IOException {
        String[] strs = FieldCache.DEFAULT.getStrings(reader, fieldName);
        xDoc = new int[strs.length];
        yDoc = new int[strs.length];
        for (int i = 0; i < strs.length; i++) {
          String[] locTuple = strs[i].split(",", -1);
          xDoc[i] = Integer.parseInt(locTuple[0]);
          yDoc[i] = Integer.parseInt(locTuple[1]);
        }
      }

      private float getDistance(int doc) {
        int deltax = xDoc[doc] - x;
        int deltay = yDoc[doc] - y;
        return (float) Math.sqrt(deltax * deltax + deltay * deltay);
      }

      public int compare(int slot1, int slot2) {
        if (values[slot1] < values[slot2]) {
          return -1;
        }
        if (values[slot1] > values[slot2]) {
          return 1;
        }
        return 0;
      }

      public void setBottom(int slot) {
        bottom = values[slot];
      }

      public int compareBottom(int doc) {
        float docDistance = getDistance(doc);
        if (bottom < docDistance) {
          return -1;
        }
        if (bottom > docDistance) {
          return 1;
        }
        return 0;
      }

      public void copy(int slot, int doc) {
        values[slot] = getDistance(doc);
      }

      public Comparable value(int slot) {
        return new Float(values[slot]);
      }


    }

    public String toString() {
      return "Distance from (" + x + "," + y + ")";
    }
  }
}