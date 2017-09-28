/*
Sampling-SQL

Copyright (c) 2015-2017 NEC Solution Innovators, Ltd.

This software is released under the MIT License, See the LICENSE file
in the project root for more information.
*/
// This is grammar definition for create sampling table.

grammar ConvertingSql;

parse
: samplekeyClause? createTableStatement ';'* EOF
;

samplekeyClause
: SAMPLE TABLE sampleTableName '(' sampleItem (',' sampleItem)* ')'
;

createTableStatement
: CREATE createOpt* TABLE (IF NOT EXISTS)? tableName '(' tableBody ')' (tableOptPG|tableOptAR*)
;

tableBody
: columnDef (',' columnDef)* (',' tableConstraint)*
;

sampleItem
: sampleColumn
| columnName AS sampleColumn
| '(' columnName (',' columnName)* ')' AS sampleColumn
;

sampleColumn
: localName
;

createOpt
: ~TABLE
;

sampleTableName
: qualifiedName
;

tableName
: qualifiedName
;

columnDef
: columnName dataType columnOpt*
;

columnName
: localName
;

columnOpt
: SORTKEY
| '(' . (',' .)* ')'
| ~(SORTKEY | ',' | ')') // | '(' | ')')
;

content
: '(' ')'
| ~(',' | '(' | ')')
| '(' content (',' content)* ')'
;

tableConstraint
: ~(IDENTIFIER | ',') content*
;

dataType
: TINYINT
| (SMALLINT | INT2)
| (INTEGER | INT | INT4)
| (BIGINT | INT8)
| (REAL | FLOAT4)
| (DOUBLE PRECISION | FLOAT8 | FLOAT)
| (DECIMAL | NUMERIC) '(' NUMERIC_LITERAL (',' NUMERIC_LITERAL)? ')'
| (CHARACTER | CHAR) '(' NUMERIC_LITERAL ')'
| (CHARACTER VARYING | VARCHAR) '(' NUMERIC_LITERAL ')'
| (BOOLEAN | BOOL)
| DATE
| TIMESTAMP
;

tableOptAR
: BACKUP (YES | NO)
| DISTSTYLE (EVEN | KEY | ALL)
| DISTKEY '(' columnName ')'
| sortkeyClause
;

sortkeyClause
: SORTKEY '(' columnName (',' columnName)* ')'
;

tableOptPG
: inheritsClause? withClause? oncommitClause? tablespaceClause? distributedClause? partitionbyClause?
;

inheritsClause
: INHERITS '(' localName (',' localName)* ')'
;

withClause
: WITH '(' withItem (',' withItem)* ')'
;

oncommitClause
: ON COMMIT (PRESERVE ROWS | DELETE ROWS | DROP)
;

tablespaceClause
: TABLESPACE localName
;

distributedClause
: DISTRIBUTED (BY '(' columnName (',' columnName)* ')' | DISTRIBUTED RANDOMLY)
;

partitionbyClause
: PARTITION BY ~(';')*
;

withItem
: . ('=' .)?
;

qualifiedName
: IDENTIFIER ('.' IDENTIFIER)*
;

localName
: IDENTIFIER
;

SAMPLE : S A M P L E;

CREATE : C R E A T E;
TABLE : T A B L E;
IF : I F;
NOT : N O T;
EXISTS : E X I S T S;
SORTKEY : S O R T K E Y;
DISTKEY : D I S T K E Y;
DISTSTYLE : D I S T S T Y L E;
DISTRIBUTED : D I S T R I B U T E D;
BY : B Y;
RANDOMLY : R A N D O M L Y;
PARTITION : P A R T I T I O N;
ALL : A L L;
KEY : K E Y;
EVEN : E V E N;
TINYINT : T I N Y I N T;
SMALLINT : S M A L L I N T;
INT2 : I N T '2';
INTEGER : I N T E G E R;
INT : I N T;
INT4 : I N T '4';
BIGINT : B I G I N T;
INT8 : I N T '8';
DECIMAL : D E C I M A L;
NUMERIC : N U M E R I C;
REAL : R E A L;
FLOAT4 : F L O A T '4';
DOUBLE : D O U B L E;
PRECISION : P R E C I S I O N;
FLOAT8 : F L O A T '8';
FLOAT : F L O A T;
BOOLEAN : B O O L E A N;
BOOL : B O O L;
CHAR : C H A R;
CHARACTER : C H A R A C T E R;
VARYING : V A R Y I N G;
VARCHAR : V A R C H A R;
DATE : D A T E;
TIMESTAMP : T I M E S T A M P;
NULL : N U L L;
UNIQUE : U N I Q U E;
PRIMARY : P R I M A R Y;
FOREIGN : F O R E I G N;
REFERENCES : R E F E R E N C E S;
CONSTRAINT : C O N S T R A I N T;
CHECK : C H E C K;
EXCLUDE : E X C L U D E;
IDENTITY : I D E N T I T Y;
DEFAULT : D E F A U L T;
AS : A S;
BACKUP : B A C K U P;
YES : Y E S;
NO : N O;
INHERITS : I N H E R I T S;
WITH : W I T H;
APPENDONLY : A P P E N D O N L Y;
BLOCKSIZE : B L O C K S I Z E;
ORIENTATION : O R I E N T A T I O N;
COLUMN : C O L U M N;
ROW : R O W;
ON : O N;
COMMIT : C O M M I T;
PRESERVE : P R E S E R V E;
ROWS : R O W S;
DELETE : D E L E T E;
DROP : D R O P;
TABLESPACE : T A B L E S P A C E;

BOOLEAN_LITERAL
: T R U E
| F A L S E
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

STRING_LITERAL
: '\'' ( ~'\'' | '\'\'' )* '\''
;

OPERATOR
: [-+*/<>=~!@#%^&|`?]
;

SINGLE_LINE_COMMENT
: '--' ~[\r\n]* -> channel(HIDDEN)
;
MULTILINE_COMMENT
: '/*' .*? ( '*/' | EOF ) -> channel(HIDDEN)
;
SPACES
: [ \u000B\t\r\n]+ -> channel(HIDDEN)
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

