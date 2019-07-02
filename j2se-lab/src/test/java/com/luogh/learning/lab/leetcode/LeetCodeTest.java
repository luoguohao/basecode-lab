package com.luogh.learning.lab.leetcode;

import org.junit.Assert;
import org.junit.Test;

public class LeetCodeTest {

  @Test
  public void testRegexPatternMatch() {
    RegexPatternMatch matcher = new RegexPatternMatch();
//    Assert.assertEquals(true, matcher.isMatch("aaaa", "a.*.a*"));
//    Assert.assertEquals(true, matcher.isMatch("aaa", "a*a"));
    Assert.assertEquals(false, matcher.isMatch("ab", ".*c"));
    Assert.assertEquals(false, matcher.isMatch("mississippi", "mis*is*p*."));
    Assert.assertEquals(true, matcher.isMatch("aab", "c*a*b"));
    Assert.assertEquals(true, matcher.isMatch("aa", ".*"));
    Assert.assertEquals(true, matcher.isMatch("aa", "a*"));
    Assert.assertEquals(false, matcher.isMatch("aa", "a"));
  }

  @Test
  public void testRegexPatternMatchUsingDpWithBottomToUp() {
    RegexPatternMatch matcher = new RegexPatternMatch();
    Assert.assertEquals(true, matcher.isMatchUsingDpWithBottomToUp("aaaa", "a.*.a*"));
    Assert.assertEquals(true, matcher.isMatchUsingDpWithBottomToUp("aaa", "a*a"));
    Assert.assertEquals(false, matcher.isMatchUsingDpWithBottomToUp("ab", ".*c"));
    Assert.assertEquals(false, matcher.isMatchUsingDpWithBottomToUp("mississippi", "mis*is*p*."));
    Assert.assertEquals(true, matcher.isMatchUsingDpWithBottomToUp("aab", "c*a*b"));
    Assert.assertEquals(true, matcher.isMatchUsingDpWithBottomToUp("aa", ".*"));
    Assert.assertEquals(true, matcher.isMatchUsingDpWithBottomToUp("aa", "a*"));
    Assert.assertEquals(false, matcher.isMatchUsingDpWithBottomToUp("aa", "a"));
  }

  @Test
  public void testRegexPatternMatchUsingDpWithUpToBottom() {
    RegexPatternMatch matcher = new RegexPatternMatch();
    Assert.assertEquals(true, matcher.isMatchUsingDpWithUpToBottom("aaaa", "a.*.a*"));
    Assert.assertEquals(true, matcher.isMatchUsingDpWithUpToBottom("aaa", "a*a"));
    Assert.assertEquals(false, matcher.isMatchUsingDpWithUpToBottom("ab", ".*c"));
    Assert.assertEquals(false, matcher.isMatchUsingDpWithUpToBottom("mississippi", "mis*is*p*."));
    Assert.assertEquals(true, matcher.isMatchUsingDpWithUpToBottom("aab", "c*a*b"));
    Assert.assertEquals(true, matcher.isMatchUsingDpWithUpToBottom("aa", ".*"));
    Assert.assertEquals(true, matcher.isMatchUsingDpWithUpToBottom("aa", "a*"));
    Assert.assertEquals(false, matcher.isMatchUsingDpWithUpToBottom("aa", "a"));
  }

  @Test
  public void testMaxWaterArea() {
    Assert.assertEquals(49, new MaxWaterArea().maxArea(new int[]{1, 8, 6, 2, 5, 4, 8, 3, 7}));
  }
}

