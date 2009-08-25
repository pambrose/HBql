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
