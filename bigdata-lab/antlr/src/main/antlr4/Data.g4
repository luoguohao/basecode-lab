grammar Data;

options {
  language = Java;
}

@header {
package com.luogh.antlr;
}


// 数据 1 10 2 20 20 3 1 2 3
file: group+ ;

group: INT sequence[$INT.int, 0]; // 一组中首位为组大小，剩余为为组元素, parse rule 可以传递参数

sequence[int n, int b]   // 给sequence()方法传参数
locals [int i = 1;] // 定义局部变量
  : ({$i <= $n}? INT {$i++;})* // match n integers
  ;

INT: [0-9]+; // match integers
WS: [ \t\n\r]+ -> skip; // toss out all whitespace