package com.luogh.learing.lab.lucene;

import java.io.IOException;

public interface SynonymEngine {

  String[] getSynonyms(String s) throws IOException;
}

