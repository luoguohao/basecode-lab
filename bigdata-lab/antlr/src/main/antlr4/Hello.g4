// define a grammar called Hello

grammar Hello;

r  : 'hello' g;         // match keyword hello followed by an identifier
g : ID|INT;
INT : [0-9]+;
ID : [a-z]+ ;             // match lower-case identifiers
WS : [ \t\r\n]+ -> skip ; // skip spaces, tabs, newlines