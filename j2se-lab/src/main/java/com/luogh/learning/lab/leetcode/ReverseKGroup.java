package com.luogh.learning.lab.leetcode;

/**
 * leetcode-25: K个一组翻转链表
 *
 * 给你一个链表，每 k 个节点一组进行翻转，请你返回翻转后的链表。 k 是一个正整数，它的值小于或等于链表的长度。 如果节点总数不是 k 的整数倍，那么请将最后剩余的节点保持原有顺序。 示例 :
 * 给定这个链表：1->2->3->4->5 当 k = 2 时，应当返回: 2->1->4->3->5 当 k = 3 时，应当返回: 3->2->1->4->5
 */
public class ReverseKGroup {

  public static void main(String[] args) {
    ListNode result = new ReverseKGroup().reverseKGroup(createTestData(), 2);
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
    ListNode node4 = new ListNode(5);

    node.next = node1;
    node1.next = node2;
    node2.next = node3;
    node3.next = node4;

    return node;
  }

  public ListNode reverseKGroup(ListNode head, int k) {
    ListNode backNode = head;
    ListNode dumpyNode = new ListNode(-1);
    ListNode next = dumpyNode;
    ListNode left = null;
    ListNode right = null;
    int index = 0;
    while (head != null) {
      if (index % k == 0) {
        left = head;
      }

      if (index % k == (k - 1)) {
        right = head;
      }

      head = head.next;

      if (left != null && right != null) {
        ListNode tmp = left;
        while (tmp != right) {
          ListNode swapNode = right.next;
          ListNode tmpNext = tmp.next;
          right.next = tmp;
          tmp.next = swapNode;
          tmp = tmpNext;
        }
        next.next = right;
        next = left;
        left = null;
        right = null;
      }
      index++;
    }

    if (dumpyNode.next == null) {
      dumpyNode.next = backNode;
    }

    return dumpyNode.next;
  }
}
