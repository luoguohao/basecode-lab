grammar Rows;

options {
  language = Java;
}

@header {
package com.luogh.antlr;
}

@parser::members { // add members to generated RowsParser
  int col;
  public RowsParser(TokenStream input, int col) { // custom constructor
    this(input);
    this.col = col;
  }
}

file: (row NL)+;

row
locals [int i=0]
  : ( STUFF
        {
          if ($i == col) System.out.println($STUFF.text);
           $i++;
        }
     )+
  ;

TAB: '\t' -> skip; // match but don't pass to the parser
NL: '\r'? '\n';    // match and pass to the parser
STUFF: ~[\t\r\n]+; // match any chars except tab,new line