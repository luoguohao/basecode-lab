lexer grammar CommonLexer;

NEWLINE : '\r'? '\n';     // lexical(token) rule
INT : [0-9]+;             // lexical(token) rule
ID : [a-z]+ ;             // lexical(token) rule
WS: [ \t\r\n]+ -> skip ; // lexical(token) rule