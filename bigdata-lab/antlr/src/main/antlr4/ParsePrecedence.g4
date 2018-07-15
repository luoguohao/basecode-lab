grammar ParsePrecedence;

// 从右到左倒序解析
expr : <assoc=right> expr '^' expr // ^ operator is right associative
  | expr '*' expr
  | expr '+' expr
  | INT
  ;

INT : [0-9]+ ;             // match lower-case identifiers
WS : [ \t\r\n]+ -> skip ;