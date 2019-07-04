package com.luogh.learning.lab.leetcode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * leetcode-17: 电话号码的字母组合
 */
public class LetterCombinations {

  Map<Character, List<String>> dicts = new HashMap<>();

  {
    dicts.put('2', Arrays.asList("a", "b", "c"));
    dicts.put('3', Arrays.asList("d", "e", "f"));
    dicts.put('4', Arrays.asList("g", "h", "i"));
    dicts.put('5', Arrays.asList("j", "k", "l"));
    dicts.put('6', Arrays.asList("m", "n", "o"));
    dicts.put('7', Arrays.asList("p", "q", "r", "s"));
    dicts.put('8', Arrays.asList("t", "u", "v"));
    dicts.put('9', Arrays.asList("w", "x", "y", "z"));
  }


  public static void main(String[] args) {
    new LetterCombinations().letterCombinations("23").forEach(System.out::println);
    new LetterCombinations().letterCombinationsUsingBacktrack("23").forEach(System.out::println);
  }


  /**
   * 使用递归回溯算法， 类似深度遍历优先
   */
  public List<String> letterCombinationsUsingBacktrack(String digits) {
    List<String> result = new ArrayList<>();
    if (!digits.isEmpty()) {
      combination("", digits, result);
    }
    return result;
  }

  private void combination(String combined, String digits, List<String> collector) {
    if (digits.isEmpty()) {
      collector.add(combined);
    } else {
      for (String str : dicts.get(digits.charAt(0))) {
        combination(combined + str, digits.substring(1), collector);
      }
    }
  }


  /**
   * 类似广度遍历优先
   */
  public List<String> letterCombinations(String digits) {

    if (digits.isEmpty()) {
      return new ArrayList<>();
    }

    List<String> result = new ArrayList<>();
    for (int i = 0; i < digits.length(); i++) {
      List<String> characters = dicts.get(digits.charAt(i));
      List<String> newList = new ArrayList<>();

      if (!result.isEmpty()) {
        for (String c : characters) {
          for (String str : result) {
            newList.add(str + c);
          }
        }
      } else {
        newList.addAll(characters);
      }
      result = newList;
    }

    return result;
  }

}
