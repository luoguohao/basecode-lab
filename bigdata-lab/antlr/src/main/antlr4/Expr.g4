grammar Expr;

options {
  language = Java;
}

@header {
package com.luogh.antlr;
}


prog: expr;
expr : multExpr (('+'|'-') multExpr)* ;
multExpr : atom (('*'|'/') atom)* ;
atom: '(' expr ')'|INT|ID;

INT : [0-9]+;
ID : [a-z]+ ;             // match lower-case identifiers
WS: [ \t\r\n]+ -> skip ; // skip spaces, tabs, newlines