package com.luogh.learing.lab.lucene.analyzer;

import java.io.Reader;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseTokenizer;
import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;

public class PositionalPorterStopAnalyzer extends Analyzer {

  private Set stopWords;

  public PositionalPorterStopAnalyzer() {
    this(StopAnalyzer.ENGLISH_STOP_WORDS_SET);
  }

  public PositionalPorterStopAnalyzer(Set stopWords) {
    this.stopWords = stopWords;
  }

  @Override
  public final TokenStream tokenStream(String fieldName, Reader reader) { // 过滤停词，以及提取词干
    StopFilter stopFilter = new StopFilter(true, new LowerCaseTokenizer(reader), stopWords);
    stopFilter.setEnablePositionIncrements(true);
    return new PorterStemFilter(stopFilter);
  }
}

