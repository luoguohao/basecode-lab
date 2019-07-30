package com.luogh.learning.lab.leetcode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * leetcode-30: 串联所有单词的字串 给定一个字符串 s 和一些长度相同的单词 words。找出 s 中恰好可以由 words 中所有单词串联形成的子串的起始位置。
 * 注意子串要与 words 中的单词完全匹配，中间不能有其他字符，但不需要考虑 words 中单词串联的顺序。
 *
 * 示例 1： 输入： s = "barfoothefoobarman", words = ["foo","bar"] 输出：[0,9] 解释： 从索引 0 和 9 开始的子串分别是
 * "barfoor" 和 "foobar" 。 输出的顺序不重要, [9,0] 也是有效答案。 示例 2：
 *
 * 输入： s = "wordgoodgoodgoodbestword", words = ["word","good","best","word"] 输出：[]
 */
public class FindSubstring {

  public static void main(String[] args) {
    List<Integer> result1 = new FindSubstring()
        .findSubstring("barfoothefoobarman", new String[]{"foo", "bar"});
    result1.forEach(System.out::println);
    System.out.println("---");

    List<Integer> result2 = new FindSubstring()
        .findSubstring("wordgoodgoodgoodbestword", new String[]{"word", "good", "best", "word"});

    result2.forEach(System.out::println);
    System.out.println("---");

    List<Integer> result = new FindSubstring()
        .findSubstring("barfoofoobarthefoobarman", new String[]{"bar", "foo", "the"});

    result.forEach(System.out::println);
    System.out.println("---");

    List<Integer> result3 = new FindSubstring()
        .findSubstring("bar1foofoobarthefoobarman", new String[]{"bar", "foo", "the"});
    result3.forEach(System.out::println);
    System.out.println("---");

    List<Integer> result4 = new FindSubstring().findSubstring("", new String[]{""});
    result4.forEach(System.out::println);
    System.out.println("---");

    List<Integer> result5 = new FindSubstring()
        .findSubstring("wordgoodgoodgoodbestword", new String[]{"word", "good", "best", "good"});
    result5.forEach(System.out::println);
    System.out.println("---");

    List<Integer> result6 = new FindSubstring()
        .findSubstring("aaaaaaaa", new String[]{"aa", "aa", "aa"});
    result6.forEach(System.out::println);
    System.out.println("---");

    List<Integer> result7 = new FindSubstring()
        .findSubstring("ababaab", new String[]{"ab", "ba", "ba"});
    result7.forEach(System.out::println);
    System.out.println("---");
  }

  public List<Integer> findSubstring(String s, String[] words) {
    List<Integer> result = new ArrayList<>();
    if (s.length() > 0 && words.length > 0) {
      backtrace(s, 0, words[0].length(), words, result);
    }
    return result;
  }

  private void backtrace(String s, int index, int wordLength, String[] words,
      List<Integer> results) {
    int wordTotalLength = wordLength * words.length;
    if (s.length() < index + wordTotalLength) {
      return;
    }

    Map<String, Integer> wordCount = new HashMap<>();
    for (String word : words) {
      wordCount.computeIfPresent(word, (key, prev) -> prev + 1);
      wordCount.putIfAbsent(word, 1);
    }

    String matchingStr = s.substring(index, index + wordTotalLength);
    boolean matched = true;
    while (!matchingStr.isEmpty()) {
      String expectedStr = matchingStr.substring(0, wordLength);
      if (wordCount.getOrDefault(expectedStr, 0) > 0) {
        wordCount.computeIfPresent(expectedStr, (k, prev) -> prev - 1);
        matchingStr = matchingStr.substring(wordLength);
      } else {
        matched = false;
        break;
      }
    }
    if (matched) {
      results.add(index);
    }
    backtrace(s, index + 1, wordLength, words, results);
  }

  /**
   * 计算结果超时
   */
  public List<Integer> findSubstring1(String s, String[] words) {
    List<Integer> result = new ArrayList<>();
    if (s.length() > 0 && words.length > 0) {
      backtrace1(s, 0, words, new HashSet<>(), result);
    }
    return result;
  }

  private void backtrace1(String s, int index, String[] words,
      Set<Integer> matchedIndexes, List<Integer> result) {
    int wordLength = words[0].length();
    if (s.length() < index || index + (words.length - matchedIndexes.size()) * wordLength > s
        .length()) {
      return;
    }
    boolean match = false;
    for (int i = 0; i < words.length; i++) {
      if (!matchedIndexes.contains(i)) {
        if (s.substring(index).startsWith(words[i])) {
          match = true;
          matchedIndexes.add(i);
          break;
        }
      }
    }

    if (match) {
      if (matchedIndexes.size() == words.length) {
        int startIndex = index - (wordLength * (words.length - 1));
        result.add(startIndex);
        backtrace1(s, startIndex + 1, words, new HashSet<>(), result);
      } else {
        backtrace1(s, index + wordLength, words, matchedIndexes, result);
      }
    } else {
      int startIndex = index - (wordLength * matchedIndexes.size()) + 1;
      backtrace1(s, startIndex, words, new HashSet<>(), result);
    }
  }
}
