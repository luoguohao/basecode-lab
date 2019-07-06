package com.luogh.learning.lab.leetcode;


/**
 * leetcode-21: 合并两个有序链表
 *
 * 输入：1->2->4, 1->3->4 输出：1->1->2->3->4->4
 */
public class MergeTwoLists {

  public static void main(String[] args) {

    ListNode node1 = new ListNode(5);

    ListNode node4 = new ListNode(1);
    ListNode node5 = new ListNode(2);
    ListNode node6 = new ListNode(4);

    node4.next = node5;
    node5.next = node6;

    ListNode next = new MergeTwoLists().mergeTwoLists(node1, node4);

    while (next != null) {
      System.out.print(next.val + "->");
      next = next.next;
    }
  }

  public ListNode mergeTwoLists(ListNode l1, ListNode l2) {
    ListNode p1 = l1;
    ListNode p2 = l2;
    ListNode head = null;
    ListNode next = null;
    while (p1 != null || p2 != null) {
      if (p1 != null && p2 != null) {
        if (p1.val <= p2.val) {
          if (head == null) {
            head = p1;
            next = head;
          } else {
            next.next = p1;
            next = next.next;
          }
          p1 = p1.next;
        } else {
          if (head == null) {
            head = p2;
            next = head;
          } else {
            next.next = p2;
            next = next.next;
          }
          p2 = p2.next;
        }
      } else if (p1 != null) {
        if (head == null) {
          head = p1;
          next = head;
        } else {
          next.next = p1;
          next = next.next;
        }
        p1 = p1.next;
      } else {
        if (head == null) {
          head = p2;
          next = head;
        } else {
          next.next = p2;
          next = next.next;
        }
        p2 = p2.next;
      }
    }
    return head;
  }


}
