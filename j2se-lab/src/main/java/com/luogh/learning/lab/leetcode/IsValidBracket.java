package com.luogh.learning.lab.leetcode;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

/**
 * leetcode-20: 有效的括号
 *
 * 给定一个只包括 '('，')'，'{'，'}'，'['，']' 的字符串，判断字符串是否有效。
 */
public class IsValidBracket {

  Map<Character, Character> chars = new HashMap<>();

  {
    chars.put('(', ')');
    chars.put('{', '}');
    chars.put('[', ']');
  }

  public boolean isValid(String s) {
    Stack<Character> stack = new Stack<>();
    for (int i = 0; i < s.length(); i++) {
      char charAt = s.charAt(i);
      if (chars.containsKey(charAt)) {
        stack.push(charAt);
      } else {
        if (stack.isEmpty()) {
          return false;
        }
        char prev = stack.pop();
        if (!chars.get(prev).equals(charAt)) {
          return false;
        }
      }
    }

    return stack.isEmpty();
  }


}
