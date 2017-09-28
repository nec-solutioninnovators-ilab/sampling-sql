/*
Sampling-SQL

Copyright (c) 2015-2017 NEC Solution Innovators, Ltd.

This software is released under the MIT License, See the LICENSE file
in the project root for more information.
*/
// This is grammar definition for sampling SQL.

grammar SamplingSql;

parse
: topSelectStmt ';'* EOF
;

topSelectStmt
: sampleClause? // allow SAMPLE clause on top level
  withClause?
  untilClause? // UNTIL clause on top level
  selectStmt
;

sampleClause
: SAMPLE sampleItem (',' sampleItem)*
;

sampleItem
: sampleTable (',' sampleTable)* BY sampleColumn
;

sampleTable
: sampleTableName (AS sampleTableAlias)?
;

sampleTableName
: qualifiedName
;

sampleTableAlias
: localName
;

sampleColumn
: localName
;

withClause
: WITH content+
;

withSelectStmt
: withClause?
  selectStmt
;

selectStmt
: selectClause
  fromClause?
  whereClause?
  untilClause? // UNTIL clause embedded
  groupClause?
  havingClause?
  orderClause?
;

selectClause
: SELECT content+
;

fromClause
: FROM fromItem (',' fromItem)*
;

fromItem
: tableName (AS? tableAlias ('(' columnAlias (',' columnAlias)* ')')?)?
| '(' selectStmt ')' AS? tableAlias ('(' columnAlias (',' columnAlias)* ')')?
| fromItem CROSS JOIN fromItem
| fromItem joinType fromItem (joinOn | joinUsing)
;

joinType
: (INNER? | (LEFT|RIGHT|FULL) OUTER?) JOIN
;

joinOn
: ON joinCondition+
;

joinCondition
: '(' contentInParen? ')'
| ~(','|ON|USING|CROSS|INNER|LEFT|RIGHT|FULL|JOIN|UNTIL|WHERE|GROUP|HAVING|ORDER|'('|')'|';')
// joinCondition allows contentInParen and tokens except for terminating ON clause
;

joinUsing
: USING '(' joinColumn (',' joinColumn)* ')'
;

tableName
: IDENTIFIER ('.' IDENTIFIER)*
;

tableAlias
: IDENTIFIER
;

columnAlias
: IDENTIFIER
;

joinColumn
: IDENTIFIER
;

whereClause
: WHERE content+
;

untilClause
: UNTIL content+
;

groupClause
: GROUP BY content+
;

havingClause
: HAVING content+
;

orderClause
: ORDER BY content+
;

// tokens immediately after structural keywords such as SELECT, WHERE, GROUP BY, and so on.
content
: '(' contentInParen? ')'
| nonStructuralWord
;

contentInParen
: withSelectStmt
| ( '(' contentInParen? ')' // if enclosed in parenthesis, refer own rule
| ~('('|')') )+?
;

nonStructuralWord
: ~(SAMPLE|WITH|UNTIL|SELECT|FROM|WHERE|GROUP|HAVING|ORDER|'('|')'|';')
;

qualifiedName
: IDENTIFIER ('.' IDENTIFIER)*
;

localName
: IDENTIFIER 
;

WITH : W I T H;
SELECT : S E L E C T;
FROM : F R O M;
WHERE : W H E R E;
SAMPLE : S A M P L E;
GROUP : G R O U P;
HAVING : H A V I N G;
ORDER : O R D E R;
UNTIL : U N T I L;
EXISTS : E X I S T S;
FOR : F O R;
BY : B Y;
AND : A N D;
OR : O R;
AS : A S;
JOIN : J O I N;
CROSS : C R O S S;
FULL : F U L L;
OUTER : O U T E R;
LEFT : L E F T;
RIGHT : R I G H T;
INNER : I N N E R;
ON : O N;
USING : U S I N G; 
TIMESTAMP : T I M E S T A M P;
TIME : T I M E;
DATE : D A T E;
INTERVAL : I N T E R V A L;
INTEGER : I N T E G E R;
SMALLINT : S M A L L I N T;
BIGINT : B I G I N T;
REAL : R E A L;
LIKE : L I K E;
ESCAPE : E S C A P E;
ASC : A S C;
DESC : D E S C;
NULLS : N U L L S;
FIRST : F I R S T;
LAST : L A S T;
IS : I S;
NOT : N O T;
BETWEEN : B E T W E E N;
SYMMETRIC : S Y M M E T R I C;
WHEN : W H E N;
THEN : T H E N;
END : E N D;
IN : I N;
CASE : C A S E;
ELSE : E L S E;
EXTRACT : E X T R A C T;

// Lexer rules for testing whether SQL contains a unsupported keyword
UNSUPPORTEDWORD
: A L T E R
| C R E A T E
| D R O P
| T R U N C A T E
| L O C K
| G R A N T
| R E V O K E
| D E L E T E
| I N S E R T
| U P D A T E
| C O P Y
| U N L O A D
| U N I O N
| I N T E R S E C T
| E X C E P T
| D E C L A R E
| F E T C H
| C L O S E
| S E T
| R E S E T
| S H O W
| E X P L A I N
| A N A L Y Z E
| V A C U U M
| P R E P A I R
| E X E C U T E
| D E A L L O C A T E
| C O M M I T
| R O L L B A C K
| S T A R T
| B E G I N
| A B O R T
| C A N C E L
| C O M M E N T
;

IDENTIFIER
: '"' (~'"' | '""')* '"'
| '`' (~'`' | '``')* '`'
| '[' ~']'* ']'
| [a-zA-Z_] [a-zA-Z_0-9$]*
;

NUMERIC_LITERAL
: DIGIT+ ( '.' DIGIT* )? ( E [-+]? DIGIT+ )?
| '.' DIGIT+ ( E [-+]? DIGIT+ )?
;

Sconst
: '\'' ( ~'\'' | '\'\'' )* '\''
;

OPERATORS
: '.'
| '::'
| '<' | '>' | '<=' | '>=' | '=' | '<>' | '!='
| '+' | '-' | '*' | '/' | '%' | '^' | '|/' | '||/' | '!' | '!!' | '@' | '&' | '|' | '#' | '~' | '<<' | '>>'
| '||'
;

SINGLE_LINE_COMMENT
: '--' ~[\r\n]* -> channel(HIDDEN)
;
MULTILINE_COMMENT
: '/*' .*? ( '*/' | EOF ) -> channel(HIDDEN)
;
SPACES
: [ \u000B\t\r\n] -> channel(HIDDEN)
;

fragment DIGIT : [0-9];

fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];
