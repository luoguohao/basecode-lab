package com.luogh.learning.lab.leetcode;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * leetcode-36: 有效的数独
 *
 * 判断一个 9x9 的数独是否有效。只需要根据以下规则，验证已经填入的数字是否有效即可。
 *
 * 数字 1-9 在每一行只能出现一次。 数字 1-9 在每一列只能出现一次。 数字 1-9 在每一个以粗实线分隔的 3x3 宫内只能出现一次。
 *
 * 一个有效的数独（部分已被填充）不一定是可解的。 只需要根据以上规则，验证已经填入的数字是否有效即可。 给定数独序列只包含数字 1-9 和字符 '.' 。 给定数独永远是 9x9 形式的。
 */
public class IsValidSudoku {

  public boolean isValidSudoku2(char[][] board) {
    boolean[][] row = new boolean[9][9];
    boolean[][] col = new boolean[9][9];
    boolean[][] block = new boolean[9][9];
    for (int i = 0; i < 9; i++) {
      for (int j = 0; j < 9; j++) {
        if (board[i][j] != '.') {
          int c = board[i][j] - '1'; // ascii码
          int blo = (i / 3) * 3 + j / 3; // 第几个小方格
          if (row[i][c] == true || col[j][c] == true || block[blo][c] == true) {
            return false;
          } else {
            row[i][c] = true;
            col[j][c] = true;
            block[blo][c] = true;
          }
        }

      }
    }
    return true;
  }

  public boolean isValidSudoku(char[][] board) {
    // 每一列的数据
    Map<Integer, Set<Character>> column = new HashMap<>();
    // 每一个小方格
    Map<String, Set<Character>> cubes = new HashMap<>();
    for (int i = 0; i < board.length; i++) {
      // 每一行的数据
      Set<Character> row = new HashSet<>();
      for (int j = 0; j < board[i].length; j++) {
        Character c = board[i][j];
        if (c != '.') {
          // 计算是第几个cube
          String cubeIndex = i / 3 + "_" + (j / 3);
          Set<Character> cube = cubes.computeIfAbsent(cubeIndex, v -> new HashSet<>());
          if (cube.contains(c)) {
            return false;
          } else {
            cube.add(c);
          }
          Set<Character> set = column.computeIfAbsent(j, v -> new HashSet<>());
          if (set.contains(c)) {
            return false;
          } else {
            set.add(c);
          }

          if (row.contains(c)) {
            return false;
          } else {
            row.add(c);
          }
        }
      }
    }
    return true;
  }
}
