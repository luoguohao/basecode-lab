package com.luogh.learing.lab.lucene;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.Scorer;

public class BookLinkCollector extends Collector {

  private Map<String, String> documents = new HashMap<String, String>();
  private Scorer scorer;
  private String[] urls;
  private String[] titles;

  public boolean acceptsDocsOutOfOrder() {
    return true;
  }

  public void setScorer(Scorer scorer) {
    this.scorer = scorer;
  }

  public void setNextReader(IndexReader reader, int docBase) throws IOException {
    urls = FieldCache.DEFAULT.getStrings(reader, "url");
    titles = FieldCache.DEFAULT.getStrings(reader, "title2");
  }

  public void collect(int docID) {
    try {
      String url = urls[docID];
      String title = titles[docID];
      documents.put(url, title);
      System.out.println(title + ":" + scorer.score());
    } catch (IOException e) {
    }
  }

  public Map<String, String> getLinks() {
    return Collections.unmodifiableMap(documents);
  }
}

