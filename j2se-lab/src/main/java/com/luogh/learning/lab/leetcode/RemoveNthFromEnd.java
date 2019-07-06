package com.luogh.learning.lab.leetcode;

/**
 * leetcode-19: 删除链表的倒数第N个节点
 */
public class RemoveNthFromEnd {

  public static void main(String[] args) {
    ListNode head = new ListNode(1);
    ListNode node2 = new ListNode(2);
    ListNode node3 = new ListNode(3);
    ListNode node4 = new ListNode(4);
    ListNode node5 = new ListNode(5);

    head.next = node2;
    node2.next = node3;
    node3.next = node4;
    node4.next = node5;

    ListNode nodes = new RemoveNthFromEnd().removeNthFromEnd(head, 5);

    ListNode next = nodes;
    while (next != null) {
      System.out.print(next.val + "->");
      next = next.next;
    }
  }

  /**
   * 快慢指针
   */
  public ListNode removeNthFromEnd(ListNode head, int n) {
    ListNode fast = head;
    ListNode slow = head;
    ListNode resultHead = null;
    int gap = 0;
    while (fast != null) {
      if (gap < n) {
        gap++;
        fast = fast.next;
      } else {
        if (resultHead == null) {
          resultHead = slow;
        }

        if (fast.next == null) {
          slow.next = (slow.next == fast) ? null : slow.next.next;
        }

        fast = fast.next;
        slow = slow.next;
      }
    }

    return resultHead == null ? slow.next : resultHead;
  }

}
