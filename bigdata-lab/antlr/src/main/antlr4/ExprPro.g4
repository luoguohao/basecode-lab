grammar ExprPro;

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

prog: stat+ ;     // parse rule
stat: expr NEWLINE | ID '=' expr NEWLINE | NEWLINE; // parse rule
expr: INT| ID | expr ('+'|'-'|'*'|'/') expr | '(' expr  ')' ; // parse rule