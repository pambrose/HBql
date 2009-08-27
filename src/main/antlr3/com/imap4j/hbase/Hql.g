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
import java.util.Date;
import com.imap4j.imap.antlr.imap.AntlrActions;
}

@lexer::header {
package com.imap4j.hbase;
import com.google.common.collect.Lists;
}

selectStmt returns [QueryArgs retval]
	: keySELECT (STAR | cols=columnList) 
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
	: keyDELETE keyFROM table=ID 
	  where=whereClause?				{retval = new DeleteArgs($table.text, $where.retval);};

setStmt returns [SetArgs retval]
	: keySET var=ID (keyTO | EQ)? val=dottedValue 	{retval = new SetArgs($var.text, $val.text);};

whereClause returns [WhereExpr retval]
	: keyWHERE c=orExpr 				{retval = new WhereExpr($c.retval);};
		
orExpr returns [OrExpr retval]
	: expr1=andExpr (keyOR expr2=orExpr)?		{retval= new OrExpr($expr1.retval, $expr2.retval);;}
	//| cond_expr keyOR cond_term
	;

andExpr returns [AndExpr retval]
	: expr1=condFactor (keyAND expr2=andExpr)?	{retval = new AndExpr($expr1.retval, $expr2.retval);}
	//| cond_term keyAND cond_factor
	;
	
condFactor returns [CondFactor retval]			 
	: k=keyNOT? p=condPrimary			{retval = new CondFactor(($k.text != null), $p.retval);}
	;

condPrimary returns [CondPrimary retval]
	: simpleCondExpr  				{retval = new CondPrimary($simpleCondExpr.retval);}
	| LPAREN orExpr RPAREN				{retval = new CondPrimary($orExpr.retval);}
	;

simpleCondExpr returns [SimpleCondExpr retval]
	: betweenExpr					//{retval = new SimpleCondExpr($betweenExpr.retval);}
	| likeExpr					//{retval = new SimpleCondExpr($likeExpr.retval);}
	| inExpr					{retval = new SimpleCondExpr($inExpr.retval);}
	| nullCompExpr					//{retval = new SimpleCondExpr($nullCompExpr.retval);}
	| compareExpr 					{retval = new SimpleCondExpr($compareExpr.retval);}
	;

betweenExpr returns [BetweenExpr retval]
	: attribRef[Number.class] keyNOT? keyBETWEEN numberExpr keyAND numberExpr
	| attribRef[String.class] keyNOT? keyBETWEEN stringExpr keyAND stringExpr
	| attribRef[Date.class] keyNOT? keyBETWEEN datetimeExpr keyAND datetimeExpr
	;

likeExpr 
	: attribRef[String.class] keyNOT? keyLIKE pattern_value=stringLiteral; // ('ESCAPE' escape_character=string_literal)?;

inExpr returns [InExpr retval]
	: a=attribRef[Number.class] n=keyNOT? keyIN 
	  LPAREN intlist=intItemList RPAREN		{retval = new IntInExpr($a.retval, ($n.text != null), $intlist.retval);} 
	| a=attribRef[String.class] n=keyNOT? keyIN 
	  LPAREN strlist=strItemList RPAREN		{retval = new StringInExpr($a.retval, ($n.text != null), $strlist.retval);} 
	;

intItem returns [Integer retval]
	: num=numberLiteral				{retval = Integer.valueOf($num.text);};

strItem : stringLiteral;

nullCompExpr
	: attribRef[String.class] keyIS (keyNOT)? keyNULL;

compareExpr returns [CompareExpr retval]
	: attribRef[String.class] compareOp stringExpr	{retval = new StringCompareExpr($attribRef.retval, $compareOp.retval, $stringExpr.retval);}
	| attribRef[Date.class] compareOp datetimeExpr 
	| attribRef[Number.class] compareOp numberExpr	{retval = new NumberCompareExpr($attribRef.retval, $compareOp.retval, $numberExpr.retval);}
	| stringExpr compareOp attribRef[String.class]	{retval = new StringCompareExpr($stringExpr.retval, $compareOp.retval, $attribRef.retval);}
	| datetimeExpr compareOp attribRef[Date.class]
	| numberExpr compareOp attribRef[Number.class]	{retval = new NumberCompareExpr($numberExpr.retval, $compareOp.retval, $attribRef.retval);}
	;
	
compareOp returns [CompareExpr.Operator retval]
	: EQ 		{retval = CompareExpr.Operator.EQ;}
	| GT 		{retval = CompareExpr.Operator.GT;}
	| GTEQ 		{retval = CompareExpr.Operator.GTEQ;}
	| LT 		{retval = CompareExpr.Operator.LT;}
	| LTEQ 		{retval = CompareExpr.Operator.LTEQ;}
	| LTGT		{retval = CompareExpr.Operator.LTGT;}
	;

numberExpr returns [NumberExpr retval]
	: simpleNumberExpr
	;

simpleNumberExpr
	: numberTerm ((PLUS | MINUS) simpleNumberExpr)?
	//| simpleNumberExpr (PLUS | MINUS) numberTerm
	;

numberTerm
	: numberFactor ((STAR | DIV) numberTerm)?
	//| numberTerm (STAR | DIV) numberFactor
	;

numberFactor
	: (PLUS | MINUS)? numberPrimary;

numberPrimary
	: numberLiteral
	| LPAREN simpleNumberExpr RPAREN
	| funcsReturningNumeric
	;

stringExpr returns [StringExpr retval]
	: lit=stringLiteral				{retval = new StringExpr($lit.retval);}
	| func=funcReturningStrings
	| attrib=attribRef[String.class]		{retval = new StringExpr($attrib.retval);}
	;

datetimeExpr
	: funcReturningDatetime
	;

funcsReturningNumeric
	: keyLENGTH LPAREN stringExpr RPAREN
	| keyABS LPAREN simpleNumberExpr RPAREN
	| keyMOD LPAREN simpleNumberExpr COMMA simpleNumberExpr RPAREN
	;

funcReturningDatetime
	: keyCURRENT_DATE
	| keyCURRENT_TIME
	| keyCURRENT_TIMESTAMP
	;

funcReturningStrings
	: keyCONCAT LPAREN stringExpr COMMA stringExpr RPAREN
	| keySUBSTRING LPAREN stringExpr COMMA simpleNumberExpr COMMA simpleNumberExpr RPAREN
	| keyTRIM LPAREN stringExpr RPAREN
	| keyLOWER LPAREN stringExpr RPAREN
	| keyUPPER LPAREN stringExpr RPAREN
	;

attribRef [Class clazz] returns [AttribRef retval]
	: v=ID 						{retval = new AttribRef(clazz, $v.text);};
		
stringLiteral returns [StringLiteral retval]
	: v=QUOTED 					{retval = new StringLiteral($v.text);};
	
numberLiteral 
	: v=INT;					{retval = Integer.valueOf($v.text);};
		
intItemList returns [List<Integer> retval]
@init {retval = Lists.newArrayList();}
	: item1=intItem {retval.add($item1.retval);} (COMMA item2=intItem {retval.add($item2.retval);})*;
	
strItemList returns [List<String> retval]
@init {retval = Lists.newArrayList();}
	: item1=strItem {retval.add($item1.text);} (COMMA item2=strItem {retval.add($item2.text);})*;
	
columnList returns [List<String> retval]
@init {retval = Lists.newArrayList();}
	: column[retval] (COMMA column[retval])*;

qstringList returns [List<String> retval]
@init {retval = Lists.newArrayList();}
	: qstring[retval] (COMMA qstring[retval])*;

column [List<String> list]	
	: charstr=dottedValue 				{if (list != null) list.add($charstr.text);};

dottedValue	
	: ID ((DOT | COLON) ID)*;

qstring	[List<String> list]
	: QUOTED 					{if (list != null) list.add($QUOTED.text);};

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
