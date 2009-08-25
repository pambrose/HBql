grammar Hql;

options {superClass=HBaseParser;}

tokens {
	DOT = '.';
	COLON = ':';
	STAR = '*';
	COMMA = ',';
	EQUALS = '=';
	DQUOTE = '"';
	SQUOTE = '\'';
	LPAREN = '(';
	RPAREN = ')';
}

@rulecatch {
catch (RecognitionException re) {
	handleRecognitionException(re);
}
}

@header {
package com.imap4j.hbase;
import com.imap4j.hbase.hql.*;
import com.imap4j.hbase.antlr.*;
import com.google.common.collect.Lists;
import com.imap4j.imap.antlr.imap.AntlrActions;
}

@lexer::header {
package com.imap4j.hbase;
import com.google.common.collect.Lists;
}

select_stmt returns [QueryArgs retval]
	: keySELECT (STAR | cols=column_list) 
	  keyFROM table=dotted_value where_clause?		{retval = new QueryArgs($cols.retval, $table.text);};

exec_cmd returns [ExecArgs retval]
	: create=create_stmt					{retval = $create.retval;}
	| desc=describe_stmt 					{retval = $desc.retval;}
	| show=show_stmt 					{retval = $show.retval;}
	| del=delete_stmt 					{retval = $del.retval;}
	| set=set_stmt						{retval = $set.retval;}
	;

create_stmt returns [CreateArgs retval]
	: keyCREATE keyTABLE table=ID 				{retval = new CreateArgs($table.text);};

describe_stmt returns [DescribeArgs retval]
	: keyDESCRIBE keyTABLE table=ID 			{retval = new DescribeArgs($table.text);};

show_stmt returns [ShowArgs retval]
	: keySHOW keyTABLES 		 			{retval = new ShowArgs();};

delete_stmt returns [DeleteArgs retval]
	: keyDELETE keyFROM table=ID where_clause?		{retval = new DeleteArgs($table.text);};

set_stmt returns [SetArgs retval]
	: keySET var=ID (keyTO | EQUALS)? val=dotted_value 	{retval = new SetArgs($var.text, $val.text);};

where_clause	
	: keyWHERE cond;
		
cond	: compare | in_stmt;

compare	: column[null] EQUALS qstring[null];

in_stmt	: column[null] keyIN LPAREN qstring_list RPAREN;

cond_expr
	: cond_term
	| cond_expr keyOR cond_term;

cond_term
	: cond_factor
	| cond_term keyAND cond_factor;
	
cond_factor
	: (keyNOT)? conditional_primary;

conditional_primary
	: simple_cond_expr | LPAREN cond_expr RPAREN;

simple_cond_expr
	: comparison_expr
	| between_expr
	| like_expr
	| in_expression
	| null_comparison_expr
	| empty_collection_comparison_expr
	| collection_member_expr
	;

between_expr
	: arithmetic_expr (keyNOT)? 'BETWEEN' arithmetic_expr keyAND arithmetic_expr
	| string_expr (keyNOT)? 'BETWEEN' string_expr keyAND string_expr
	| datetime_expr (keyNOT)? 'BETWEEN' datetime_expr keyAND datetime_expr;

in_expression
	: state_field_path_expr (keyNOT)? 'IN' LPAREN  (in_item (COMMA in_item)*) RPAREN;

in_item
	: string_literal
	| numeric_literal
	;

like_expr
	: string_expr keyNOT? 'LIKE' pattern_value=string_literal ('ESCAPE' escape_character=string_literal)?;

null_comparison_expr
	: single_valued_path_expression 'IS' (keyNOT)? 'NULL';

empty_collection_comparison_expr
	: collection_valued_path_expression 'IS' (keyNOT)? 'EMPTY';

collection_member_expr
	: entity_expr keyNOT? 'MEMBER' ('OF')? collection_valued_path_expression;

comparison_expr
	: string_expr comparison_operator (string_expr)
	| boolean_expr ('=' | '<>') (boolean_expr)
	| datetime_expr comparison_operator (datetime_expr)
	| entity_expr ('=' | '<>') (entity_expr)
	| arithmetic_expr comparison_operator (arithmetic_expr);

comparison_operator
	: '='
	| '>'
	| '>='
	| '<'
	| '<='
	| '<>';

arithmetic_expr
	: simple_arithmetic_expr
	;

simple_arithmetic_expr
	: arithmetic_term
	| simple_arithmetic_expr ( '+' | '-' ) arithmetic_term;

arithmetic_term
	: arithmetic_factor
	| arithmetic_term ( '*' | '/' ) arithmetic_factor;

arithmetic_factor
	: ( '+' | '-' )? arithmetic_primary;

arithmetic_primary
	: state_field_path_expr
	| numeric_literal
	| LPAREN simple_arithmetic_expr RPAREN
	| functions_returning_numerics
	;

string_expr
	: string_primary 
	;

string_primary
	: state_field_path_expr
	| string_literal
	| functions_returning_strings
	;

datetime_expr
	: datetime_primary
	;

datetime_primary
	: state_field_path_expr
	| functions_returning_datetime
	;

boolean_expr
	: boolean_primary
	;

boolean_primary
	: state_field_path_expr
	| boolean_literal
	;

entity_expr
	: single_valued_association_path_expression
	| simple_entity_expression;

simple_entity_expression
	: identification_variable
	;

functions_returning_numerics
	: 'LENGTH' LPAREN string_primary RPAREN
	| 'LOCATE' LPAREN string_primary COMMA string_primary (COMMA simple_arithmetic_expr)? RPAREN
	| 'ABS' LPAREN simple_arithmetic_expr RPAREN
	| 'SQRT' LPAREN simple_arithmetic_expr RPAREN
	| 'MOD' LPAREN simple_arithmetic_expr COMMA simple_arithmetic_expr RPAREN
	| 'SIZE' LPAREN collection_valued_path_expression RPAREN;

functions_returning_datetime
	: 'CURRENT_DATE'
	| 'CURRENT_TIME'
	| 'CURRENT_TIMESTAMP';

functions_returning_strings
	: 'CONCAT' LPAREN string_primary COMMA string_primary RPAREN
	| 'SUBSTRING' LPAREN string_primary COMMA simple_arithmetic_expr  simple_arithmetic_expr RPAREN
	| 'TRIM' LPAREN string_primary RPAREN
	| 'LOWER' LPAREN string_primary RPAREN
	| 'UPPER' LPAREN string_primary RPAREN;

single_valued_path_expression
	: state_field_path_expr 
	| single_valued_association_path_expression
	;

state_field_path_expr
	: (identification_variable | single_valued_association_path_expression) DOT state_field
	;

single_valued_association_path_expression
	: identification_variable DOT (single_valued_association_field=ID DOT)* single_valued_association_field;

collection_valued_path_expression
	: identification_variable DOT (single_valued_association_field=ID DOT)* collection_valued_association_field=ID;

state_field
	: (embedded_class_state_field=ID DOT)* simple_state_field=ID;

identification_variable
	: ID;
	
boolean_literal
	: keyTRUE 
	| keyFALSE
	;
	
string_literal
	: QUOTED;
	
numeric_literal 
	: INT;
		
column_list returns [List<String> retval]
@init {retval = Lists.newArrayList();}
	: column[retval] (COMMA column[retval])*;

qstring_list returns [List<String> retval]
@init {retval = Lists.newArrayList();}
	: qstring[retval] (COMMA qstring[retval])*;

column [List<String> list]	
	: charstr=dotted_value 					{if (list != null) list.add($charstr.text);};

dotted_value	
	: ID ((DOT | COLON) ID)*;

qstring	[List<String> list]
	: QUOTED 						{if (list != null) list.add($QUOTED.text);};

INT	: DIGIT+;
ID	: CHAR (CHAR | DIGIT)*;
 
QUOTED		
@init {final StringBuffer sbuf = new StringBuffer();}	
	: DQUOTE (options {greedy=false;} : any=. {sbuf.append((char)$any);})* DQUOTE 	{setText(sbuf.toString());}
	| SQUOTE (options {greedy=false;} : any=. {sbuf.append((char)$any);})* SQUOTE	{setText(sbuf.toString());}
	;

fragment
DIGIT	: '0'..'9'; 

fragment
CHAR 	: 'a'..'z' | 'A'..'Z'; 

WS 	: (' ' |'\t' |'\n' |'\r' )+ {skip();} ;

keySELECT 	: {AntlrActions.isKeyword(input, "SELECT")}? ID;
keyDELETE 	: {AntlrActions.isKeyword(input, "DELETE")}? ID;
keyCREATE 	: {AntlrActions.isKeyword(input, "CREATE")}? ID;
keyDESCRIBE 	: {AntlrActions.isKeyword(input, "DESCRIBE")}? ID;
keySHOW 	: {AntlrActions.isKeyword(input, "SHOW")}? ID;
keyTABLE 	: {AntlrActions.isKeyword(input, "TABLE")}? ID;
keyTABLES 	: {AntlrActions.isKeyword(input, "TABLES")}? ID;
keyWHERE	: {AntlrActions.isKeyword(input, "WHERE")}? ID;
keyFROM 	: {AntlrActions.isKeyword(input, "FROM")}? ID;
keySET 		: {AntlrActions.isKeyword(input, "SET")}? ID;
keyIN 		: {AntlrActions.isKeyword(input, "IN")}? ID;
keyTO 		: {AntlrActions.isKeyword(input, "TO")}? ID;
keyOR 		: {AntlrActions.isKeyword(input, "OR")}? ID;
keyAND 		: {AntlrActions.isKeyword(input, "AND")}? ID;
keyNOT 		: {AntlrActions.isKeyword(input, "NOT")}? ID;
keyTRUE 	: {AntlrActions.isKeyword(input, "TRUE")}? ID;
keyFALSE 	: {AntlrActions.isKeyword(input, "FALSE")}? ID;
