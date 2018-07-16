grammar CSV;

import CommonLexer;

file: hdr row+;
hdr: row;

row: field (',' field)* '\r'? '\n';
field: TEXT | STRING| ;

TEXT: ~[,\n\r"]+;
STRING: '"' ('""'|~'"')* '"'; // quote-quote is an escaped quote
//STRING: '"' ('""'|.)*? '"' ; // quote-quote is an escaped quote