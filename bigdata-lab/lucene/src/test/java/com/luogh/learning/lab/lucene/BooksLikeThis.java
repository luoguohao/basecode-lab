package com.luogh.learning.lab.lucene;

import com.luogh.learning.lab.lucene.common.TestUtil;
import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;

public class BooksLikeThis {

  public static void main(String[] args) throws IOException {
    Directory dir = TestUtil.getBookIndexDirectory();
    IndexReader reader = IndexReader.open(dir);
    int numDocs = reader.maxDoc();

    BooksLikeThis blt = new BooksLikeThis(reader);

    for (int i = 0; i < numDocs; i++) {
      System.out.println();
      Document doc = reader.document(i);
      System.out.println(doc.get("title"));
      Document[] docs = blt.docsLike(i, 10);

      if (docs.length == 0) {
        System.out.println(" None like this");
      }

      for (Document likeThisDoc : docs) {
        System.out.println(" -> " + likeThisDoc.get("title"));
      }
    }
    reader.close();
    dir.close();
  }

  private IndexReader reader;
  private IndexSearcher searcher;

  public BooksLikeThis(IndexReader reader) {
    this.reader = reader;
    searcher = new IndexSearcher(reader);
  }

  public Document[] docsLike(int id, int max) throws IOException {
    Document doc = reader.document(id);
    String[] authors = doc.getValues("author");
    BooleanQuery authorQuery = new BooleanQuery();

    for (String author : authors) {
      authorQuery.add(new TermQuery(new Term("author", author)), BooleanClause.Occur.SHOULD);
    }

    authorQuery.setBoost(2.0f);

    TermFreqVector vector = reader.getTermFreqVector(id, "subject");
    BooleanQuery subjectQuery = new BooleanQuery();

    for (String vecTerm : vector.getTerms()) {
      TermQuery tq = new TermQuery(new Term("subject", vecTerm));
      subjectQuery.add(tq, BooleanClause.Occur.SHOULD);
    }

    BooleanQuery likeThisQuery = new BooleanQuery();
    likeThisQuery.add(authorQuery, BooleanClause.Occur.SHOULD);
    likeThisQuery.add(subjectQuery, BooleanClause.Occur.SHOULD);

    likeThisQuery
        .add(new TermQuery(new Term("isbn", doc.get("isbn"))), BooleanClause.Occur.MUST_NOT);

    TopDocs hits = searcher.search(likeThisQuery, 10);

    int size = max;

    if (max > hits.scoreDocs.length) {
      size = hits.scoreDocs.length;
    }

    Document[] docs = new Document[size];

    for (int i = 0; i < size; i++) {
      docs[i] = reader.document(hits.scoreDocs[i].doc);
    }

    return docs;
  }
}

