package com.luogh.learning.lab.leetcode;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * leetcode-23: 合并K个排序链表 输入: [ 1->4->5, 1->3->4, 2->6 ] 输出: 1->1->2->3->4->4->5->6
 */
public class MergeKLists {

  public static ListNode[] createTestData() {
    ListNode node1 = new ListNode(1);
    ListNode node12 = new ListNode(4);
    ListNode node13 = new ListNode(5);

    node1.next = node12;
    node12.next = node13;

    ListNode node2 = new ListNode(1);
    ListNode node22 = new ListNode(3);
    ListNode node23 = new ListNode(4);

    node2.next = node22;
    node22.next = node23;

    ListNode node3 = new ListNode(7);
    ListNode node31 = new ListNode(8);
    node3.next = node31;
    return new ListNode[]{node1, node2, node3};
  }
  public static void main(String[] args) {
    ListNode result = new MergeKLists().mergeKLists(createTestData());
    while (result != null) {
      System.out.print(result.val + "->");
      result = result.next;
    }

    System.out.println();
    System.out.println("=========");

    ListNode result2 = new MergeKLists()
        .mergeKListsWithPriorityQueue(createTestData());
    while (result2 != null) {
      System.out.print(result2.val + "->");
      result2 = result2.next;
    }

    System.out.println();
    System.out.println("=========");

    ListNode result3 = new MergeKLists()
        .mergeKListsWithMerger(createTestData());
    while (result3 != null) {
      System.out.print(result3.val + "->");
      result3 = result3.next;
    }
  }


  /**
   * 算法1. 比较 k 个节点（每个链表的首节点），获得最小值的节点。 将选中的节点接在最终有序链表的后面。 时间复杂度为：O(n*k)
   */
  public ListNode mergeKLists(ListNode[] lists) {
    ListNode[] rootNode = new ListNode[1];
    mergeKLists(null, lists, rootNode);
    return rootNode[0];
  }

  public void mergeKLists(ListNode nextNode, ListNode[] lists, ListNode[] rootNode) {
    ListNode minNode = null;
    int index = -1;
    for (int i = 0; i < lists.length; i++) {
      ListNode curNode = lists[i];
      if (curNode != null) {
        minNode = minNode == null ? curNode : (minNode.val > curNode.val ? curNode : minNode);
        if (minNode == curNode) {
          index = i;
        }
      }
    }

    if (minNode == null) {
      return;
    }

    lists[index] = lists[index].next;

    if (nextNode == null) {
      rootNode[0] = minNode;
      mergeKLists(minNode, lists, rootNode);
    } else {
      nextNode.next = minNode;
      nextNode = nextNode.next;
      mergeKLists(nextNode, lists, rootNode);
    }
  }

  /**
   * 算法2. 用优先队列优化方法 O(Nlogk)，这里N是这k个链表的结点总数，每一次从一个优先队列中选出一个最小结点的时间复杂度是 O(logk)， 故时间复杂度为O(Nlogk)。
   */
  public ListNode mergeKListsWithPriorityQueue(ListNode[] lists) {
    PriorityQueue<ListNode> nodes = new PriorityQueue<>(
        Comparator.comparing(x -> x.val)); // 值越小，优先级越高
    for (ListNode node : lists) {
      if (node != null) {
        nodes.add(node);
      }
    }

    ListNode dummyNode = new ListNode(-1);
    ListNode nextNode = dummyNode;
    while (!nodes.isEmpty()) {
      ListNode minNode = nodes.poll();
      nextNode.next = minNode;
      nextNode = nextNode.next;
      if (minNode.next != null) {
        nodes.add(minNode.next);
      }
    }


    return dummyNode.next;
  }

  /**
   * 算法三： 分治思想，先一分为二，分别“递归地”解决了与原问题同结构，但规模更小的两个子问题； 再考虑如何合并，这个合并的过程也是一个递归方法，代码结构和“归并排序”可以说是同出一辙。
   *
   * 使用归并思路获取各个链表中的最小值
   */
  public ListNode mergeKListsWithMerger(ListNode[] lists) {
    if (lists.length == 0) {
      return null;
    }
    return mergeKListsWithMerger(lists, 0, lists.length - 1);
  }

  private ListNode mergeKListsWithMerger(ListNode[] lists, int l, int r) {
    if (l == r) {
      return lists[l];
    }

    int mid = (r - l) / 2 + l;
    ListNode leftNode = mergeKListsWithMerger(lists, l, mid);
    ListNode rightNode = mergeKListsWithMerger(lists, mid + 1, r);
    return mergeKLists(leftNode, rightNode);
  }

  private ListNode mergeKLists(ListNode leftNode, ListNode rightNode) {
    if (leftNode == null) {
      return rightNode;
    }

    if (rightNode == null) {
      return leftNode;
    }

    if (leftNode.val < rightNode.val) {
      leftNode.next = mergeKLists(leftNode.next, rightNode);
      return leftNode;
    }

    rightNode.next = mergeKLists(leftNode, rightNode.next);
    return rightNode;
  }


}