package com.luogh.learing.lab.lucene.analyzer;

import java.io.IOException;
import java.io.Reader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.Version;

public class SynonymAnalyzer extends Analyzer {

  private SynonymEngine engine;

  public SynonymAnalyzer(SynonymEngine engine) {
    this.engine = engine;
  }

  public final TokenStream tokenStream(String fieldName, Reader reader) {
    TokenStream result = new SynonymFilter(new StopFilter(true,
        new LowerCaseFilter(new StandardFilter(new StandardTokenizer(Version.LUCENE_30, reader))),
        StopAnalyzer.ENGLISH_STOP_WORDS_SET), engine);

    return result;
  }

  @Override
  public final TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
    return super.reusableTokenStream(fieldName, reader);
  }
}

