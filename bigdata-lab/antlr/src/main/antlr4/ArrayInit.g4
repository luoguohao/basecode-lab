grammar ArrayInit;

options {
  language = Java;
}

@header {
package com.luogh.antlr;
}


init: '{' value (',' value)* '}';
value: INT | init ;

INT : [0-9]+;
ID : [a-z]+ ;             // match lower-case identifiers
WS: [ \t\r\n]+ -> skip ; // skip spaces, tabs, newlines