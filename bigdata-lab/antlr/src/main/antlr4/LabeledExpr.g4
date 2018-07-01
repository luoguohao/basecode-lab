grammar LabeledExpr;

import CommonLexer;

options {
  language = Java;
}

@header {
package com.luogh.antlr;
}


/**
* match:
* newline
* 154
* a = 145
* a = (154 + 23) * 2
* 2 * (a * b) * 2
*/

prog: stat+
;

stat: expr NEWLINE    # printExpr    // using '#' to create a label for the parse rule
    | ID '=' expr NEWLINE # assign
    | NEWLINE  # blank
    ;


expr: INT # int
    | ID  # id
    | expr opt=(MUL|DIV|ADD|SUB) expr # operation
    | '(' expr  ')' # parens
    ;

/*define some constants for the java code usage*/
MUL : '*';  // assigns token name to '*' used above in grammar '/' ;
DIV : '/';
ADD : '+';
SUB : '-';