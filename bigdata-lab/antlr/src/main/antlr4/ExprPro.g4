grammar ExprPro;

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
expr: atom | expr ('+'|'-'|'*'|'/') expr | '(' expr  ')' ; // parse rule
atom: INT|ID; // parse rule

NEWLINE : '\r'? '\n';     // lexical(token) rule
INT : [0-9]+;             // lexical(token) rule
ID : [a-z]+ ;             // lexical(token) rule
WS: [ \t\r\n]+ -> skip ; // lexical(token) rule