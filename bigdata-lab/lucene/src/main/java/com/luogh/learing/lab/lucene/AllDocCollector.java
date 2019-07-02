package com.luogh.learing.lab.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;

public class AllDocCollector extends Collector {

  List<ScoreDoc> docs = new ArrayList<ScoreDoc>();
  private Scorer scorer;
  private int docBase;

  public boolean acceptsDocsOutOfOrder() {
    return false;
  }

  public void setScorer(Scorer scorer) {
    this.scorer = scorer;
  }

  public void setNextReader(IndexReader reader, int docBase) {
    this.docBase = docBase;
  }

  public void collect(int doc) throws IOException {
    docs.add(new ScoreDoc(doc + docBase, scorer.score()));
  }

  public void reset() {
    docs.clear();
  }

  public List<ScoreDoc> getHits() {
    return docs;
  }
}

