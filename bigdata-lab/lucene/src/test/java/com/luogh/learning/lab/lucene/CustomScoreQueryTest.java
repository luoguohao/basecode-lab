package com.luogh.learning.lab.lucene;

import com.luogh.learning.lab.lucene.common.TestUtil;
import java.io.IOException;
import java.util.Date;
import junit.framework.TestCase;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.function.CustomScoreProvider;
import org.apache.lucene.search.function.CustomScoreQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

/**
 * 自定义打分
 */
public class CustomScoreQueryTest extends TestCase {

  public void testRecency() throws Throwable {
    Directory dir = TestUtil.getBookIndexDirectory();
    IndexReader r = IndexReader.open(dir);
    IndexSearcher s = new IndexSearcher(r);
    s.setDefaultFieldSortScoring(true, true);

    QueryParser parser = new QueryParser(Version.LUCENE_30, "contents",
        new StandardAnalyzer(Version.LUCENE_30));

    Query q = parser.parse("java in action");
    Query q2 = new RecencyBoostingQuery(q, 2.0, 2 * 365, "pubmonth");

    Sort sort = new Sort(
        SortField.FIELD_SCORE, new SortField("title2", SortField.STRING));

    TopDocs hits = s.search(q2, null, 5, sort);

    for (int i = 0; i < hits.scoreDocs.length; i++) {
      Document doc = r.document(hits.scoreDocs[i].doc);
      System.out.println(
          (1 + i) + ": " + doc.get("title") + ": pubmonth=" + doc.get("pubmonth") + " score="
              + hits.scoreDocs[i].score);
    }

    s.close();
    r.close();
    dir.close();
  }


  static class RecencyBoostingQuery extends CustomScoreQuery {

    double multiplier;
    int today;
    int maxDaysAgo;
    String dayField;
    static int MSEC_PER_DAY = 1000 * 3600 * 24;

    public RecencyBoostingQuery(Query q, double multiplier, int maxDaysAgo, String dayField) {
      super(q);
      today = (int) (new Date().getTime() / MSEC_PER_DAY);
      this.multiplier = multiplier;
      this.maxDaysAgo = maxDaysAgo;
      this.dayField = dayField;
    }

    @Override
    public CustomScoreProvider getCustomScoreProvider(IndexReader r) throws IOException {
      return new RecencyBooster(r);
    }

    private class RecencyBooster extends CustomScoreProvider {

      final int[] publishDay;

      public RecencyBooster(IndexReader r) throws IOException {
        super(r);
        publishDay = FieldCache.DEFAULT.getInts(r, dayField);
      }

      @Override
      public float customScore(int doc, float subQueryScore, float valSrcScore) {
        int daysAgo = today - publishDay[doc];
        if (daysAgo < maxDaysAgo) {
          float boost = (float) (multiplier * (maxDaysAgo - daysAgo) / maxDaysAgo);

          return (float) (subQueryScore * (1.0 + boost));
        } else {
          return subQueryScore;
        }
      }
    }
  }

}
