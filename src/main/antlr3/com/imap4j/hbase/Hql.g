grammar Hql;

options {
	superClass=HBaseParser;
	backtrack=true;
}

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
import com.imap4j.hbase.hql.expr.*;
import com.imap4j.hbase.antlr.*;
import com.google.common.collect.Lists;
import com.imap4j.imap.antlr.imap.AntlrActions;
}

@lexer::header {
package com.imap4j.hbase;
import com.google.common.collect.Lists;
}

selectStmt returns [QueryArgs retval]
	: keySELECT (STAR | cols=column_list) 
	  keyFROM table=dottedValue 
	  where=whereClause?				{retval = new QueryArgs($cols.retval, $table.text, $where.retval);};

execCommand returns [ExecArgs retval]
	: create=createStmt				{retval = $create.retval;}
	| desc=describeStmt 				{retval = $desc.retval;}
	| show=showStmt 				{retval = $show.retval;}
	| del=deleteStmt 				{retval = $del.retval;}
	| set=setStmt					{retval = $set.retval;}
	;

createStmt returns [CreateArgs retval]
	: keyCREATE keyTABLE table=ID 			{retval = new CreateArgs($table.text);};

describeStmt returns [DescribeArgs retval]
	: keyDESCRIBE keyTABLE table=ID 		{retval = new DescribeArgs($table.text);};

showStmt returns [ShowArgs retval]
	: keySHOW keyTABLES 		 		{retval = new ShowArgs();};

deleteStmt returns [DeleteArgs retval]
	: keyDELETE keyFROM table=ID whereClause?	{retval = new DeleteArgs($table.text);};

setStmt returns [SetArgs retval]
	: keySET var=ID (keyTO | EQ)? val=dottedValue 	{retval = new SetArgs($var.text, $val.text);};

whereClause returns [CondExpr retval]
	: keyWHERE c=condExpr 				{retval = $c.retval;};
		
condExpr returns [CondExpr retval]
@init {retval = new CondExpr();}
	: term=condTerm (keyOR expr=condExpr)?
	{
	 retval.term = $term.retval;
	 retval.expr = $expr.retval;
	}
	//| cond_expr keyOR cond_term
	;

condTerm returns [CondTerm retval]
@init {retval = new CondTerm();}
	: factor=condFactor (keyAND term=condTerm)?
	{
	 retval.factor = $factor.retval;
	 retval.term = $term.retval;	 
	}
	//| cond_term keyAND cond_factor
	;
	
condFactor returns [CondFactor retval]
@init {retval = new CondFactor();}
	: keyNOT? primary=condPrimary
	{
	  retval.not = $keyNOT.text != null;
	  retval.primary = $primary.retval;
	}
	;

condPrimary returns [CondPrimary retval]
@init {retval = new CondPrimary();}
	: simpleCondExpr  		{retval.expr = $simpleCondExpr.retval;}
	| LPAREN condExpr RPAREN	{retval.expr = $condExpr.retval;}
	;

simpleCondExpr returns [SimpleCondExpr retval]
@init {retval = new SimpleCondExpr();}
	: betweenExpr			//{retval.expr = $betweenExpr.retval;}
	| likeExpr			//{retval.expr = $likeExpr.retval;}
	| inExpr			{retval.expr = $inExpr.retval;}
	| nullCompExpr			//{retval.expr = $nullCompExpr.retval;}
	| compExpr 			{retval.expr = $compExpr.retval;}
	;

betweenExpr returns [BetweenExpr retval]
	: attribField keyNOT? keyBETWEEN 
	  ( arithmeticExpr keyAND arithmeticExpr
	  | stringExpr keyAND stringExpr
	  | datetimeExpr keyAND datetimeExpr
	  )
	;

likeExpr 
	: attribField keyNOT? keyLIKE pattern_value=stringLiteral; // ('ESCAPE' escape_character=string_literal)?;

inExpr returns [InExpr retval]
@init {retval = new InExpr();}
	: attrib=attribField keyNOT? keyIN LPAREN (intlist=intItemList | strlist=strItemList) RPAREN
	{
	 retval.attrib = $attrib.text; 
	 retval.not = $keyNOT.text != null; 
	 retval.intList = $intlist.retval;
	 retval.strList = $strlist.retval;
	}
	;

intItemList returns [List<Integer> retval]
@init {retval = Lists.newArrayList();}
	: item1=intItem {retval.add($item1.retval);} (COMMA item2=intItem {retval.add($item2.retval);})*;
	
strItemList returns [List<String> retval]
@init {retval = Lists.newArrayList();}
	: item1=strItem {retval.add($item1.text);} (COMMA item2=strItem {retval.add($item2.text);})*;
	
intItem returns [Integer retval]
	: num=numericLiteral		{retval = Integer.valueOf($num.text);};

strItem : stringLiteral;

nullCompExpr
	: attribField keyIS (keyNOT)? keyNULL;

compExpr returns [CompExpr retval]
@init {retval = new CompExpr();}
	: attrib=attribField op=compareOp (stringExpr | datetimeExpr | arithmeticExpr)
	{
	 retval.attrib = $attrib.text;
	 retval.op = $op.retval;
	}
	| (stringExpr | datetimeExpr | arithmeticExpr)  compareOp attribField
	;
	
compareOp	returns [CompExpr.Operator retval]
	: EQ 		{retval = CompExpr.Operator.EQ;}
	| GT 		{retval = CompExpr.Operator.GT;}
	| GTEQ 		{retval = CompExpr.Operator.GTEQ;}
	| LT 		{retval = CompExpr.Operator.LT;}
	| LTEQ 		{retval = CompExpr.Operator.LTEQ;}
	| LTGT		{retval = CompExpr.Operator.LTGT;}
	;

arithmeticExpr
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
	: numericLiteral
	| LPAREN simple_arithmetic_expr RPAREN
	| funcs_returning_numerics
	;

stringExpr
	: string_primary 
	;

string_primary
	: stringLiteral
	| funcs_returning_strings
	;

datetimeExpr
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

attribField
	: ID;
		
stringLiteral
	: QUOTED;
	
numericLiteral 
	: INT;
		
column_list returns [List<String> retval]
@init {retval = Lists.newArrayList();}
	: column[retval] (COMMA column[retval])*;

qstring_list returns [List<String> retval]
@init {retval = Lists.newArrayList();}
	: qstring[retval] (COMMA qstring[retval])*;

column [List<String> list]	
	: charstr=dottedValue 					{if (list != null) list.add($charstr.text);};

dottedValue	
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
keyCURRENT_TIMESTAMP : {AntlrActions.isKeyword(input, "CURRENT_TIMESTAMP")}? ID;
