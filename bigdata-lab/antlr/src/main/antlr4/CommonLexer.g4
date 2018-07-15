lexer grammar CommonLexer;

NEWLINE : '\r'? '\n';     // lexical(token) rule
WS: [ \t\r\n]+ -> skip ; // lexical(token) rule

// Comments
LINE_COMMENT : '//' .*? '\n' -> skip;
COMMENT      : '/*' .*? '*/' -> skip;

// Strings
STRING       : '"' (ESC | .)*? '"';
fragment ESC  : '\\' [btnr"\\] ; // \b \t \n \r " \

// Numbers
INT : DIGIT+ ;
FLOAT: DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;
fragment DIGIT: [0-9];

// Identifiers
ID : ID_LETTER (ID_LETTER | DIGIT)*;
fragment ID_LETTER: [a-zA-Z_];   // a-z 等价于 'a'..'z'

// Keywords: reversed identifiers
//Punctuation
LP: '(';
RP: ')';


