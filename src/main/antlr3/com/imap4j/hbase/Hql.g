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
		: keySELECT (STAR | column_list) keyFROM table=dotted_value 
		{retval = new QueryArgs($column_list.retval, $table.text);}
		;

exec_cmd returns [ExecArgs retval]
		: stmt=create_stmt	{retval = $stmt.retval;}
		| stmt=describe_stmt 	{retval = $stmt.retval;}
		| stmt=delete_stmt 	{retval = $stmt.retval;}
		| stmt=set_stmt		{retval = $stmt.retval;}
		;

create_stmt returns [CreateArgs retval]
		: keyCREATE keyTABLE table=ID 
		{retval = new CreateArgs($table.text);}
		;

describe_stmt returns [DescribeArgs retval]
		: keyDESCRIBE keyTABLE table=ID 
		{retval = new DescribeArgs($table.text);}
		;

delete_stmt returns [DeleteArgs retval]
		: keyDELETE keyFROM table=ID (keyWHERE condition)? 
		{retval = new DeleteArgs($table.text);}
		;

set_stmt returns [SetArgs retval]
		: keySET var=ID (keyTO | EQUALS)? val=dotted_value 
		{retval = new SetArgs($var.text, $val.text);}
		;
		
condition	: compare | in_stmt;

compare		: column[null] EQUALS qstring[null];

in_stmt		: column[null] keyIN LPAREN qstring_list RPAREN;
		
column_list returns [List<String> retval]
@init {retval = Lists.newArrayList();}
		: column[retval] (COMMA column[retval])*;

qstring_list returns [List<String> retval]
@init {retval = Lists.newArrayList();}
		: qstring[retval] (COMMA qstring[retval])*;

column [List<String> list]	
		: charstr=dotted_value {if (list != null) list.add($charstr.text);};

dotted_value	: ID ((DOT | COLON) ID)*;

qstring	[List<String> list]
		: (DQUOTE charstr=ID DQUOTE) {if (list != null) list.add($charstr.text);}
		| (SQUOTE charstr=ID SQUOTE) {if (list != null) list.add($charstr.text);};

INT		: DIGIT+;
ID	 	: CHAR (CHAR | DIGIT)*;
 
fragment
DIGIT		: '0'..'9'; 

fragment
CHAR 		: 'a'..'z' | 'A'..'Z'; 

WS 		: (' ' |'\t' |'\n' |'\r' )+ {skip();} ;

keySELECT 		: {AntlrActions.isKeyword(input, "SELECT")}? ID;
keyDELETE 		: {AntlrActions.isKeyword(input, "DELETE")}? ID;
keyCREATE 		: {AntlrActions.isKeyword(input, "CREATE")}? ID;
keyDESCRIBE 		: {AntlrActions.isKeyword(input, "DESCRIBE")}? ID;
keyTABLE 		: {AntlrActions.isKeyword(input, "TABLE")}? ID;
keyWHERE		: {AntlrActions.isKeyword(input, "WHERE")}? ID;
keyFROM 		: {AntlrActions.isKeyword(input, "FROM")}? ID;
keySET 			: {AntlrActions.isKeyword(input, "SET")}? ID;
keyIN 			: {AntlrActions.isKeyword(input, "IN")}? ID;
keyTO 			: {AntlrActions.isKeyword(input, "TO")}? ID;
