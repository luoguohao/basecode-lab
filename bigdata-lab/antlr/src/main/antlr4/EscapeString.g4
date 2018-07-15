grammar EscapeString;

escape: '"' (ESC|.)*? '"' ;

ESC : '\\"' | '\\\\' ; // 2-char sequences \" and \\