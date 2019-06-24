package com.luogh.learing.lab.lucene;

import java.io.IOException;
import java.io.Reader;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LetterTokenizer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;

public class StopAnalyzer2 extends Analyzer {

  private Set stopWords;

  public StopAnalyzer2() {
    stopWords = StopAnalyzer.ENGLISH_STOP_WORDS_SET;
  }

  public StopAnalyzer2(String[] stopWords) {
    this.stopWords = StopFilter.makeStopSet(stopWords);
  }

  public final TokenStream tokenStream(String fieldName, Reader reader) {
    return new StopFilter(true, new LowerCaseFilter(new LetterTokenizer(reader)), stopWords);
  }

  @Override
  public final TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
    return super.reusableTokenStream(fieldName, reader);
  }
}

