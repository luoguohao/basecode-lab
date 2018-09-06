package com.luogh.j2se.test;

import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.Test;

public class TopicGenerator {

  @Test
  public void test() throws Exception {
    Files.lines(Paths.get(TopicGenerator.class.getClassLoader().getResource("topics").toURI()))
        .forEach(x -> {
          StringBuilder stringBuilder = new StringBuilder();
          stringBuilder.append("{").append("\"topic\"").append(":").append("\"").append(x).append("\"").append("},");
          System.out.println(stringBuilder.toString());
        });
  }

}
