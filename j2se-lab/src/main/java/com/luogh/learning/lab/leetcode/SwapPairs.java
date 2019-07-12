package com.luogh.learning.lab.leetcode;

/**
 * leetcode-24: 两两交换链表中的节点
 *
 * 给定一个链表，两两交换其中相邻的节点，并返回交换后的链表。 你不能只是单纯的改变节点内部的值，而是需要实际的进行节点交换。
 *
 *
 * 示例:
 *
 * 给定 1->2->3->4, 你应该返回 2->1->4->3.
 */
public class SwapPairs {

  public static void main(String[] args) {
    ListNode result = new SwapPairs().swapPairs(createTestData());
    while (result != null) {
      System.out.print(result.val + "->");
      result = result.next;
    }
  }

  private static ListNode createTestData() {
    ListNode node = new ListNode(1);
    ListNode node1 = new ListNode(2);
    ListNode node2 = new ListNode(3);
    ListNode node3 = new ListNode(4);

    node.next = node1;
    node1.next = node2;
    node2.next = node3;

    return node;
  }

  public ListNode swapPairs(ListNode head) {
    if (head == null) {
      return null;
    }

    if (head.next == null) {
      return head;
    }

    ListNode p = null;
    ListNode q = null;
    ListNode result = new ListNode(-1);
    ListNode next = result;
    int i = 0;
    while (head != null) {
      if (i % 2 == 0) {
        p = head;
      } else {
        q = head;
      }

      head = head.next;
      if (p != null && q != null) {
        p.next = q.next;
        next.next = q;
        q.next = p;
        next = p;
        p = null;
        q = null;
      }
      i++;
    }
    return result.next;
  }
}
