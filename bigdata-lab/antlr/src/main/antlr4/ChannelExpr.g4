grammar ChannelExpr;

options {
  language = Java;
}

@header {
package com.luogh.antlr;
}

import CommonLexer;

prog: INT* ID* (COMMENT)* NEWLINE;

COMMENT: '/*' .*? '*/' -> channel(HIDDEN) // match anything between /* and */
;

WS : [ \r\t\u000C\n]+ -> channel(HIDDEN);