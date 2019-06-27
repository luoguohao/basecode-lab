package com.luogh.learning.lab.lucene;

import com.luogh.learning.lab.lucene.common.TestUtil;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DecimalFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

public class SortingExample {

  private Directory directory;

  public SortingExample(Directory directory) {
    this.directory = directory;
  }

  public void displayResults(Query query, Sort sort) throws IOException {
    IndexSearcher searcher = new IndexSearcher(directory);

    /**
     * By default, the search method that accepts a Sort argument won’t compute any scores
     * for the matching documents. This is often a sizable performance gain, and many applications
     * don’t need the scores when sorting by field. If scores aren’t needed in your application,
     * it’s best to keep this default. If you need to change the default, use IndexSearcher’s
     * setDefaultFieldSortScoring method, which takes two Booleans: doTrackScores and doMaxScore.
     * If doTrackScores is true, then each hit will have a score computed. If doMaxScore is true,
     * then the max score across all hits will be computed. Note that computing the max score is
     * in general more costly than the score per hit, because the score per hit is only computed
     * if the hit is competitive.
     *
     */
    searcher.setDefaultFieldSortScoring(true, false);

    TopDocs results = searcher.search(query, null, 20, sort);

    System.out.println("\nResults for: " + query.toString() + " sorted by " + sort);
    System.out.println(
        StringUtils.rightPad("Title", 30) + StringUtils.rightPad("Title2", 10)
            + StringUtils.rightPad("pubmonth", 10) + StringUtils
            .center("id", 4) + StringUtils.center("score", 15));

    PrintStream out = new PrintStream(System.out, true, "UTF-8");
    DecimalFormat scoreFormatter = new DecimalFormat("0.######");
    for (ScoreDoc sd : results.scoreDocs) {
      int docID = sd.doc;
      float score = sd.score;
      Document doc = searcher.doc(docID);
      System.out.println(
          StringUtils.rightPad(StringUtils.abbreviate(doc.get("title"), 29), 30)
           + StringUtils.rightPad(StringUtils.abbreviate(doc.get("title2") != null ? doc.get("title2") : "<空>", 29), 10)
              + StringUtils.rightPad(doc.get("pubmonth"), 10)
              + StringUtils.center("" + docID, 4)
              + StringUtils.leftPad(scoreFormatter.format(score), 12));
      out.println(" " + doc
          .get("category"));
      //out.println(searcher.explain(query, docID));
    }
    searcher.close();
  }

  public static void main(String[] args) throws Exception {
    Query allBooks = new MatchAllDocsQuery();
    QueryParser parser = new QueryParser(Version.LUCENE_30, "contents",
        new StandardAnalyzer(Version.LUCENE_30));
    BooleanQuery query = new BooleanQuery();
    query.add(allBooks, BooleanClause.Occur.SHOULD);
    query.add(parser.parse("java OR action"), BooleanClause.Occur.SHOULD);
    Directory directory = TestUtil.getBookIndexDirectory();
    SortingExample example = new SortingExample(directory);
    example.displayResults(query, Sort.RELEVANCE);
    example.displayResults(query, Sort.INDEXORDER);
    example.displayResults(query, new Sort(new SortField("title2", SortField.STRING)));
    example.displayResults(query, new Sort(new SortField("pubmonth", SortField.INT, true)));

    example.displayResults(query,
        new Sort(new SortField("category", SortField.STRING), SortField.FIELD_SCORE,
            new SortField("pubmonth", SortField.INT, true)));
    example.displayResults(query, new Sort(
        SortField.FIELD_SCORE, new SortField("category", SortField.STRING)));
    directory.close();
  }

}