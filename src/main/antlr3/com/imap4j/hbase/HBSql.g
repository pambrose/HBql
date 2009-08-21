grammar HBSql;

options {superClass=HBaseParser;}

tokens {
	SELECT = 'select';
	FROM = 'from';
	DELETE = 'delete';
	WHERE = 'where';
	IN = 'in';
	COMMA = ',';
	EQUALS = '=';
	DOT = '.';
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
import com.imap4j.hbase.hbsql.*;
import com.imap4j.hbase.antlr.*;
import com.google.common.collect.Lists;
}

@lexer::header {
package com.imap4j.hbase;
import com.google.common.collect.Lists;
}

query returns [QueryArgs retval]
		: SELECT column_list FROM table
		{retval = new QueryArgs($column_list.retval, $table.text);};

delete returns [DeleteArgs retval]
		: DELETE column_list FROM table WHERE condition 
		;

condition	: compare 
		| in_stmt
		;

compare		: column[null] EQUALS qstring[null];

in_stmt		: column[null] IN LPAREN qstring_list RPAREN;
		
column_list returns [List<String> retval]
@init {retval = Lists.newArrayList();}
		: column[retval] (COMMA column[retval])*;

qstring_list returns [List<String> retval]
@init {retval = Lists.newArrayList();}
		: qstring[retval] (COMMA qstring[retval])*;

column [List<String> list]	
		: ATOM {if (list != null) list.add($ATOM.text);};

qstring	[List<String> list]
		: (DQUOTE ATOM DQUOTE) {if (list != null) list.add($ATOM.text);}
		| (SQUOTE ATOM SQUOTE) {if (list != null) list.add($ATOM.text);};

table		: ATOM;

ATOM 		: CHAR (CHAR | DIGIT | DOT)*;

fragment 
DIGIT : '0'..'9'; 

fragment 
CHAR : 'a'..'z' | 'A'..'Z'; 

WS : (' ' |'\t' |'\n' |'\r' )+ {skip();} ;
