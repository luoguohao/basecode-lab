package com.luogh.learning.lab.lucene;

import com.luogh.learning.lab.lucene.common.TestUtil;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import junit.framework.TestCase;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermFreqVector;

/**
 * 根据词向量来计算两个document的相似度
 */
public class TermVectorTest extends TestCase {

  private TreeMap categoryMap;

  protected void setUp() throws Exception {
    categoryMap = new TreeMap();
    buildCategoryVectors();
  }


  public void testCategorization() throws Exception {
    assertEquals("/technology/computers/programming/methodology",
        getCategory("extreme agile methodology"));
    assertEquals("/education/pedagogy", getCategory("montessori education philosophy"));
  }

  private String getCategory(String subject) {
    String[] words = subject.split(" ");
    Iterator categoryIterator = categoryMap.keySet().iterator();
    double bestAngle = Double.MAX_VALUE;
    String bestCategory = null;
    while (categoryIterator.hasNext()) {
      String category = (String) categoryIterator.next();
      double angle = computeAngle(words, category);
      if (angle < bestAngle) {
        bestAngle = angle;
        bestCategory = category;
      }
    }
    return bestCategory;
  }

  /**
   * 计算每个词的余弦相似度
   * @param words
   * @param category
   * @return
   */
  private double computeAngle(String[] words, String category) {
    Map vectorMap = (Map) categoryMap.get(category);
    int dotProduct = 0;
    int sumOfSquares = 0;
    for (String word : words) {
      int categoryWordFreq = 0;
      if (vectorMap.containsKey(word)) {
        categoryWordFreq = (Integer) vectorMap.get(word);
      }
      dotProduct += categoryWordFreq;
      sumOfSquares += categoryWordFreq * categoryWordFreq;
    }
    double denominator;
    if (sumOfSquares == words.length) {
      denominator = sumOfSquares;
    } else {
      denominator = Math.sqrt(sumOfSquares) * Math.sqrt(words.length);
    }
    double ratio = dotProduct / denominator;
    return Math.acos(ratio);
  }


  private void buildCategoryVectors() throws IOException {
    IndexReader reader = IndexReader.open(TestUtil.getBookIndexDirectory());
    int maxDoc = reader.maxDoc();
    for (int i = 0; i < maxDoc; i++) {
      if (!reader.isDeleted(i)) {
        Document doc = reader.document(i);
        String category = doc.get("category");
        Map vectorMap = (Map) categoryMap.get(category);
        if (vectorMap == null) {
          vectorMap = new TreeMap();
          categoryMap.put(category, vectorMap);
        }
        TermFreqVector termFreqVector = reader.getTermFreqVector(i, "subject");
        addTermFreqToMap(vectorMap, termFreqVector);
      }
    }
  }

  private void addTermFreqToMap(Map vectorMap, TermFreqVector termFreqVector) {
    String[] terms = termFreqVector.getTerms();
    int[] freqs = termFreqVector.getTermFrequencies();
    for (int i = 0; i < terms.length; i++) {
      String term = terms[i];
      if (vectorMap.containsKey(term)) {
        Integer value = (Integer) vectorMap.get(term);
        vectorMap.put(term, value + freqs[i]);
      } else {
        vectorMap.put(term, freqs[i]);
      }
    }
  }


}
