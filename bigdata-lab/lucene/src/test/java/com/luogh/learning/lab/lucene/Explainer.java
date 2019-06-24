package com.luogh.learning.lab.lucene;

import com.luogh.learning.lab.lucene.common.TestUtil;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

public class Explainer {

  public static void main(String[] args) throws Exception {
    String queryExpression = "java";
    Directory directory = TestUtil.getBookIndexDirectory();

    QueryParser parser = new QueryParser(Version.LUCENE_30, "contents", new SimpleAnalyzer());
    Query query = parser.parse(queryExpression);
    System.out.println("Query: " + queryExpression);
    IndexSearcher searcher = new IndexSearcher(directory);
    TopDocs topDocs = searcher.search(query, 10);
    for (ScoreDoc match : topDocs.scoreDocs) {
      Explanation explanation = searcher.explain(query, match.doc);
      System.out.println("----------");
      Document doc = searcher.doc(match.doc);
      System.out.println("docId:" + match.doc);
      System.out.println("title:" + doc.get("title"));
      System.out.println(explanation.toString());
    }
    searcher.close();
    directory.close();
  }
}
