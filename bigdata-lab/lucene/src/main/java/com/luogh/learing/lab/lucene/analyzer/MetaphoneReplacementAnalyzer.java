package com.luogh.learing.lab.lucene.analyzer;

import com.luogh.learing.lab.lucene.MetaphoneReplacementFilter;
import java.io.IOException;
import java.io.Reader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LetterTokenizer;
import org.apache.lucene.analysis.TokenStream;

public class MetaphoneReplacementAnalyzer extends Analyzer {

  public final TokenStream tokenStream(String fieldName, Reader reader) {
    return new MetaphoneReplacementFilter(
        new LetterTokenizer(reader));
  }

  @Override
  public final TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
    return super.reusableTokenStream(fieldName, reader);
  }

}

