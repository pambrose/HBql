grammar HBSql;

options {superClass=HBaseParser;}

tokens {
	SELECT = 'select';
	FROM = 'from';
	COMMA = ',';
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
}

@lexer::header {
package com.imap4j.hbase;
}

query returns [QueryRequest retval]
		: SELECT column_list FROM table;

column_list	: column (COMMA column)*;

column		: ATOM;

table		: ATOM;

ATOM 		: CHAR (CHAR | DIGIT)*;

fragment 
DIGIT : '0'..'9'; 

fragment 
CHAR : 'a'..'z' | 'A'..'Z'; 

WS : (' ' |'\t' |'\n' |'\r' )+ {skip();} ;
