package com.luogh.learning.lab.leetcode;

/**
 * leetcode-37: 解数独
 */
public class SolveSudoku {

  public static void main(String[] args) {
    char[][] chars = new char[][]{
        new char[]{'5', '3', '.', '.', '7', '.', '.', '.', '.'},
        new char[]{'6', '.', '.', '1', '9', '5', '.', '.', '.'},
        new char[]{'.', '9', '8', '.', '.', '.', '.', '6', '.'},
        new char[]{'8', '.', '.', '.', '6', '.', '.', '.', '3'},
        new char[]{'4', '.', '.', '8', '.', '3', '.', '.', '1'},
        new char[]{'7', '.', '.', '.', '2', '.', '.', '.', '6'},
        new char[]{'.', '6', '.', '.', '.', '.', '2', '8', '.'},
        new char[]{'.', '.', '.', '4', '1', '9', '.', '.', '5'},
        new char[]{'.', '.', '.', '.', '8', '.', '.', '7', '9'}
    };

    new SolveSudoku().solveSudoku(chars);

    Utils.printArray(chars);
  }

  /**
   * 回溯法
   * @param board
   */
  public void solveSudoku(char[][] board) {
    boolean[][] rows = new boolean[9][9];
    boolean[][] columns = new boolean[9][9];
    boolean[][] cubes = new boolean[9][9];

    for (int i = 0; i < 9; i++) {
      for (int j = 0; j < 9; j++) {
        int c = board[i][j];
        int index = c - '1';
        int cubeIndex = (i / 3) * 3 + j / 3;
        if (c != '.') {
          rows[i][index] = true;
          columns[j][index] = true;
          cubes[cubeIndex][index] = true;
        }
      }
    }

    backtrace(board, rows, columns, cubes, 0, 0);
  }

  private boolean backtrace(char[][] board, boolean[][] rows, boolean[][] columns,
      boolean[][] cubes, int i, int j) {
    if (i > 8 || j > 8) {
      return true;
    }

    if (board[i][j] == '.') {
      int cubeIndex = (i / 3) * 3 + j / 3;
      for (int k = 0; k < 9; k++) {
        if (!rows[i][k] && !columns[j][k] && !cubes[cubeIndex][k]) { // 候选值
          rows[i][k] = true;
          columns[j][k] = true;
          cubes[cubeIndex][k] = true;
          board[i][j] = (char) ('1' + k);

          // 下一个cell判断
          int nextI = j == 8 ? i + 1 : i;
          int nextJ = j == 8 ? 0 : j + 1;
          boolean matched = backtrace(board, rows, columns, cubes, nextI, nextJ);
          if (!matched) {
            rows[i][k] = false;
            columns[j][k] = false;
            cubes[cubeIndex][k] = false;
            board[i][j] = '.';
          } else {
            return true;
          }
        }
      }
      return false;
    } else {
      int nextI = j == 8 ? i + 1 : i;
      int nextJ = j == 8 ? 0 : j + 1;
      return backtrace(board, rows, columns, cubes, nextI, nextJ);
    }
  }
}
