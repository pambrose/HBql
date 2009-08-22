grammar HBql;

options {superClass=HBaseParser;}

tokens {
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
import com.imap4j.hbase.hbql.*;
import com.imap4j.hbase.antlr.*;
import com.google.common.collect.Lists;
import com.imap4j.imap.antlr.imap.AntlrActions;
}

@lexer::header {
package com.imap4j.hbase;
import com.google.common.collect.Lists;
}

query_stmt returns [QueryArgs retval]
		: keySELECT column_list keyFROM table=ID
		{retval = new QueryArgs($column_list.retval, $table.text);};

delete_stmt returns [DeleteArgs retval]
		: keyDELETE column_list keyFROM ID keyWHERE condition 
		;

set_stmt returns [SetArgs retval]
		: keySET var=ID (keyTO | EQUALS) val=ID
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
		: charstr=ID {if (list != null) list.add($charstr.text);};

qstring	[List<String> list]
		: (DQUOTE charstr=ID DQUOTE) {if (list != null) list.add($charstr.text);}
		| (SQUOTE charstr=ID SQUOTE) {if (list != null) list.add($charstr.text);};

ID	 	: CHAR (CHAR | DIGIT | '.' | ':')*;
 
fragment
DIGIT		: '0'..'9'; 

fragment
CHAR 		: 'a'..'z' | 'A'..'Z'; 

WS 		: (' ' |'\t' |'\n' |'\r' )+ {skip();} ;

keySELECT 		: {AntlrActions.isKeyword(input, "SELECT")}? ID;
keyDELETE 		: {AntlrActions.isKeyword(input, "DELETE")}? ID;
keyWHERE		: {AntlrActions.isKeyword(input, "WHERE")}? ID;
keyFROM 		: {AntlrActions.isKeyword(input, "FROM")}? ID;
keySET 			: {AntlrActions.isKeyword(input, "SET")}? ID;
keyIN 			: {AntlrActions.isKeyword(input, "IN")}? ID;
keyTO 			: {AntlrActions.isKeyword(input, "TO")}? ID;