grammar Hql;

options {superClass=HBaseParser;backtrack=true;}

tokens {
	DOT = '.';
	COLON = ':';
	STAR = '*';
	DIV = '/';
	COMMA = ',';
	PLUS = '+';
	MINUS = '-';
	EQ = '=';
	LT = '<';
	GT = '>';
	LTGT = '<>';	
	LTEQ = '<=';	
	GTEQ = '>=';	
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
	: keySET var=ID (keyTO | EQ)? val=dotted_value 	{retval = new SetArgs($var.text, $val.text);};

where_clause	
	: keyWHERE cond_expr;
		
cond_expr
	: cond_term (keyOR cond_expr)?
	//| cond_expr keyOR cond_term
	;

cond_term
	: cond_factor (keyAND cond_term)?
	//| cond_term keyAND cond_factor
	;
	
cond_factor
	: (keyNOT)? cond_primary;

cond_primary
	: simple_cond_expr 
	| LPAREN cond_expr RPAREN
	;

simple_cond_expr
	: between_expr
	| like_expr
	| in_expr
	| null_comp_expr
	| comp_expr 
	;

between_expr
	: attr_field keyNOT? keyBETWEEN 
	  ( arithmetic_expr keyAND arithmetic_expr
	  | string_expr keyAND string_expr
	  | datetime_expr keyAND datetime_expr
	  )
	;

like_expr
	: attr_field keyNOT? keyLIKE pattern_value=string_literal; // ('ESCAPE' escape_character=string_literal)?;

in_expr	: attr_field keyNOT? keyIN LPAREN in_item (COMMA in_item)* RPAREN;

in_item : string_literal | numeric_literal;

null_comp_expr
	: attr_field keyIS (keyNOT)? keyNULL;

comp_expr
	: attr_field comp_op (string_expr | datetime_expr | arithmetic_expr)
	//| (string_expr | datetime_expr | arithmetic_expr)  comp_op attr_field
	;
	
comp_op	: EQ | GT | GTEQ | LT | LTEQ | LTGT;

arithmetic_expr
	: simple_arithmetic_expr
	;

simple_arithmetic_expr
	: arithmetic_term ((PLUS | MINUS) simple_arithmetic_expr)?
	//| simple_arithmetic_expr (PLUS | MINUS) arithmetic_term
	;

arithmetic_term
	: arithmetic_factor ((STAR | DIV) arithmetic_term)?
	//| arithmetic_term (STAR | DIV) arithmetic_factor
	;

arithmetic_factor
	: ( PLUS | MINUS )? arithmetic_primary;

arithmetic_primary
	: numeric_literal
	| LPAREN simple_arithmetic_expr RPAREN
	| funcs_returning_numerics
	;

string_expr
	: string_primary 
	;

string_primary
	: string_literal
	| funcs_returning_strings
	;

datetime_expr
	: funcs_returning_datetime
	;

funcs_returning_numerics
	: keyLENGTH LPAREN string_primary RPAREN
	| keyABS LPAREN simple_arithmetic_expr RPAREN
	| keyMOD LPAREN simple_arithmetic_expr COMMA simple_arithmetic_expr RPAREN
	;

funcs_returning_datetime
	: keyCURRENT_DATE
	| keyCURRENT_TIME
	| keyCURRENT_TIMESTAMP;

funcs_returning_strings
	: keyCONCAT LPAREN string_primary COMMA string_primary RPAREN
	| keySUBSTRING LPAREN string_primary COMMA simple_arithmetic_expr COMMA simple_arithmetic_expr RPAREN
	| keyTRIM LPAREN string_primary RPAREN
	| keyLOWER LPAREN string_primary RPAREN
	| keyUPPER LPAREN string_primary RPAREN
	;

attr_field
	: ID;
		
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
keyIS 		: {AntlrActions.isKeyword(input, "IS")}? ID;
keyLIKE		: {AntlrActions.isKeyword(input, "LIKE")}? ID;
keyTO 		: {AntlrActions.isKeyword(input, "TO")}? ID;
keyOR 		: {AntlrActions.isKeyword(input, "OR")}? ID;
keyAND 		: {AntlrActions.isKeyword(input, "AND")}? ID;
keyNOT 		: {AntlrActions.isKeyword(input, "NOT")}? ID;
keyBETWEEN 	: {AntlrActions.isKeyword(input, "BETWEEN")}? ID;
keyNULL 	: {AntlrActions.isKeyword(input, "NULL")}? ID;
keyLOWER 	: {AntlrActions.isKeyword(input, "LOWER")}? ID;
keyUPPER 	: {AntlrActions.isKeyword(input, "UPPER")}? ID;
keyTRIM 	: {AntlrActions.isKeyword(input, "TRIM")}? ID;
keyCONCAT 	: {AntlrActions.isKeyword(input, "CONCAT")}? ID;
keySUBSTRING 	: {AntlrActions.isKeyword(input, "SUBSTRING")}? ID;
keyLENGTH 	: {AntlrActions.isKeyword(input, "LENGTH")}? ID;
keyABS 		: {AntlrActions.isKeyword(input, "ABS")}? ID;
keyMOD	 	: {AntlrActions.isKeyword(input, "MOD")}? ID;
keyCURRENT_DATE	: {AntlrActions.isKeyword(input, "CURRENT_DATE")}? ID;
keyCURRENT_TIME : {AntlrActions.isKeyword(input, "CURRENT_TIME")}? ID;
keyCURRENT_TIMESTAMP 	: {AntlrActions.isKeyword(input, "CURRENT_TIMESTAMP")}? ID;
