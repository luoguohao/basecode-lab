package com.luogh.learing.lab.lucene.analyzer;

import java.io.IOException;

public interface SynonymEngine {

  String[] getSynonyms(String s) throws IOException;
}

