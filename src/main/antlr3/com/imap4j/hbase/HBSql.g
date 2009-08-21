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
import com.google.common.collect.Lists;
}

@lexer::header {
package com.imap4j.hbase;
import com.google.common.collect.Lists;
}

query returns [QueryArgs retval]
		: SELECT column_list FROM table
		{
		  retval = new QueryArgs($column_list.retval, $table.text);
		}
		;

column_list returns [List<String> retval]
@init {retval = Lists.newArrayList();}
		: column[retval] (COMMA column[retval])*;

column [List<String> list]	
		: ATOM {list.add($ATOM.text);};


table		: ATOM;

ATOM 		: CHAR (CHAR | DIGIT)*;

fragment 
DIGIT : '0'..'9'; 

fragment 
CHAR : 'a'..'z' | 'A'..'Z'; 

WS : (' ' |'\t' |'\n' |'\r' )+ {skip();} ;
