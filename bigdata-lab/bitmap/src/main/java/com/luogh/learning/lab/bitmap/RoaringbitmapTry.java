package com.luogh.learning.lab.bitmap;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import org.roaringbitmap.RoaringBitmap;

public class RoaringbitmapTry {

  public static void main(String[] args) throws Exception {
    RoaringBitmap rr = RoaringBitmap.bitmapOf(1,2,3,1000);
    RoaringBitmap rr2 = new RoaringBitmap();
    rr2.add(0, Integer.MAX_VALUE);
    rr.select(3); // would return the third value or 1000
    rr.rank(2); // would return the rank of 2, which is index 1
    rr.contains(1000); // will return true
    rr.contains(7); // will return false

    RoaringBitmap rror = RoaringBitmap.or(rr, rr2);// new bitmap
    rr.or(rr2); //in-place computation
    boolean equals = rror.equals(rr);// true
    if(!equals) throw new RuntimeException("bug");
    // number of values stored?
    long cardinality = rr.getLongCardinality();
    System.out.println("cardinality: " + cardinality);

    try (OutputStream fileOutputStream = new FileOutputStream(new File("test"))) {
      rr.serialize(new DataOutputStream(fileOutputStream));
    }

    RoaringBitmap rr3 = new RoaringBitmap();

    try (InputStream fileInputStream = new FileInputStream(new File("/tmp/push_bitmap/bloom-filter-bitmap_123"))) {
      rr3.deserialize(new DataInputStream(fileInputStream));
    }

    System.out.println("deserialize bitmap cardinality:" + rr3.getCardinality());

  }
}
