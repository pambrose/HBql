grammar HBql;

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
	BANGEQ = '!=';	
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
import com.imap4j.hbase.hbql.expr.*;
import com.imap4j.hbase.antlr.args.*;
import com.imap4j.hbase.antlr.*;
import java.util.Date;
import com.google.common.collect.Lists;
}

@lexer::header {
package com.imap4j.hbase;
import com.google.common.collect.Lists;
}

selectStmt returns [QueryArgs retval]
	: keySELECT (STAR | cols=columnList) 
	  keyFROM table=dottedValue 
	  where=whereClause?				{retval = new QueryArgs($cols.retval, $table.text, $where.retval);};

columnList returns [List<String> retval]
@init {retval = Lists.newArrayList();}
	: column[retval] (COMMA column[retval])*;

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
	: keySET i=ID (keyTO | EQ)? v=dottedValue 	{retval = new SetArgs($i.text, $v.text);};

whereClause returns [WhereExpr retval]
	: keyWHERE e=orExpr 				{retval = new WhereExpr($e.retval);};
		
orExpr returns [PredicateExpr retval]
	: e1=andExpr (keyOR e2=orExpr)?			{retval= new OrExpr($e1.retval, $e2.retval);;};

andExpr returns [PredicateExpr retval]
	: e1=condFactor (keyAND e2=andExpr)?		{retval = new AndExpr($e1.retval, $e2.retval);};
	
condFactor returns [PredicateExpr retval]			 
	: k=keyNOT? p=condPrimary			{retval = new CondFactor(($k.text != null), $p.retval);};

condPrimary returns [PredicateExpr retval]
	: s=simpleCondExpr  				{retval = $s.retval;}
	| LPAREN o=orExpr RPAREN			{retval = $o.retval;}
	;

simpleCondExpr returns [PredicateExpr retval]
	: b=betweenStmt					{retval = $b.retval;}
	| l=likeStmt					//{retval = $l.retval;}
	| i=inStmt					{retval = $i.retval;}
	| b=booleanStmt					{retval = $b.retval;}
	| n=nullCompExpr				{retval = $n.retval;}
	| c=compareExpr 				{retval = $c.retval;}
	;

betweenStmt returns [PredicateExpr retval]
	: n1=numericExpr n=keyNOT? keyBETWEEN
	  n2=numericExpr keyAND n3=numericExpr		{retval = new BetweenStmt(ExprType.IntegerType, $n1.retval, ($n.text != null), $n2.retval, $n3.retval);}
	| s1=stringExpr n=keyNOT? keyBETWEEN
	  s2=stringExpr keyAND s3=stringExpr		{retval = new BetweenStmt(ExprType.StringType, $s1.retval, ($n.text != null), $s2.retval, $s3.retval);}
	| d1=dateExpr n=keyNOT? keyBETWEEN
	  d2=dateExpr keyAND d3=dateExpr
	;

likeStmt 
	: stringExpr 
	  keyNOT? keyLIKE pattern_value=stringExpr; // ('ESCAPE' escape_character=string_literal)?;

inStmt returns [PredicateExpr retval]
	: a1=numericExpr n=keyNOT? keyIN 
	  LPAREN i=intItemList RPAREN			{retval = new InStmt(ExprType.IntegerType, $a1.retval, ($n.text != null), $i.retval);} 
	| a2=stringExpr n=keyNOT? keyIN 
	  LPAREN s=strItemList RPAREN			{retval = new InStmt(ExprType.StringType, $a2.retval, ($n.text != null), $s.retval);} 
	;

booleanStmt returns [PredicateExpr retval]
	: b=booleanExpr					{retval = new BooleanStmt($b.retval);};
	
nullCompExpr returns [PredicateExpr retval]
	: a=stringExpr keyIS (n=keyNOT)? keyNULL	{retval = new NullCompare(($n.text != null), $a.retval);};	

compareExpr returns [PredicateExpr retval]
	: s1=stringExpr o=compOp s2=stringExpr	  	{retval = new StringCompare($s1.retval, $o.retval, $s2.retval);}
	| d1=dateExpr o=compOp d2=dateExpr 
	| n1=numericExpr o=compOp n2=numericExpr	{retval = new NumberCompare($n1.retval, $o.retval, $n2.retval);}
	;
	
compOp returns [CompareExpr.Operator retval]
	: EQ EQ?					{retval = CompareExpr.Operator.EQ;}
	| GT 						{retval = CompareExpr.Operator.GT;}
	| GTEQ 						{retval = CompareExpr.Operator.GTEQ;}
	| LT 						{retval = CompareExpr.Operator.LT;}
	| LTEQ 						{retval = CompareExpr.Operator.LTEQ;}
	| (LTGT | BANGEQ)				{retval = CompareExpr.Operator.NOTEQ;}
	;

// Numeric calculations
numericExpr returns [ValueExpr retval]
	: n1=multdivExpr (op=plusMinus n2=numericExpr)?
	{$numericExpr.retval= ($n2.text == null) ? new CalcExpr($n1.retval) : new CalcExpr($n1.retval, $op.retval, $n2.retval);}
	;

multdivExpr returns [ValueExpr retval]
	: n1=calcNumberExpr (op=multDiv n2=multdivExpr)?
	{$multdivExpr.retval = ($n2.text == null) ? new CalcExpr($n1.retval) : new CalcExpr($n1.retval, $op.retval, $n2.retval);}
	;

calcNumberExpr returns [ValueExpr retval]
	: (s=plusMinus)? n=numberPrimary 		
	{retval = ($s.retval == CalcExpr.OP.MINUS) ? new CalcExpr($n.retval, CalcExpr.OP.NEGATIVE, null) : $n.retval;}
	;

numberPrimary returns [ValueExpr retval]
	: n=numberExpr					{retval = $n.retval;}
	| LPAREN s=numericExpr RPAREN			{retval = $s.retval;}
	;

// Simple typed exprs
numberExpr returns [ValueExpr retval]
	: f=funcReturningNumber
	| l=numberLiteral				{retval = $l.retval;} 
	| i=intAttrib					{retval = $i.retval;}
	;

stringExpr returns [ValueExpr retval]
	: s=stringLiteral				{retval = $s.retval;}
	| f=funcReturningString
	| n=keyNULL					{retval = new NullLiteral();}
	| a=strAttrib					{retval = $a.retval;}
	;

booleanExpr returns [ValueExpr retval]
	: b=booleanLiteral				{retval = $b.retval;}
	//| f=funcReturningBoolean
	;

dateExpr returns [ValueExpr retval]
	: funcReturningDatetime
	;

// Attribs with type
strAttrib returns [ValueExpr retval]
	: a=attribRef[ExprType.StringType] 		{retval = $a.retval;};

intAttrib returns [ValueExpr retval]
	: a=attribRef[ExprType.NumberType] 		{retval = $a.retval;};

dateAttrib returns [ValueExpr retval]
	: a=attribRef[ExprType.DateType] 		{retval = $a.retval;};

attribRef [ExprType type] returns [ValueExpr retval]
	: v=ID 						{retval = new AttribRef(type, $v.text);};

// Literals		
stringLiteral returns [ValueExpr retval]
	: v=QUOTED 					{retval = new StringLiteral($v.text);};
	
numberLiteral returns [ValueExpr retval]
	: v=INT						{retval = new NumberLiteral(Integer.valueOf($v.text));};
		
booleanLiteral returns [ValueExpr retval]
	: t=keyTRUE					{retval = new BooleanLiteral($t.text);}
	| f=keyFALSE					{retval = new BooleanLiteral($f.text);}
	;

funcReturningNumber
	: keyLENGTH LPAREN stringExpr RPAREN
	| keyABS LPAREN numericExpr RPAREN
	| keyMOD LPAREN numericExpr COMMA numericExpr RPAREN
	;

funcReturningDatetime
	: keyCURRENT_DATE
	| keyCURRENT_TIME
	| keyCURRENT_TIMESTAMP
	;

funcReturningString
	: keyCONCAT LPAREN stringExpr COMMA stringExpr RPAREN
	| keySUBSTRING LPAREN stringExpr COMMA numericExpr COMMA numericExpr RPAREN
	| keyTRIM LPAREN stringExpr RPAREN
	| keyLOWER LPAREN stringExpr RPAREN
	| keyUPPER LPAREN stringExpr RPAREN
	;
	
funcReturningBoolean
	: 
	;
		
intItemList returns [List<Object> retval]
@init {retval = Lists.newArrayList();}
	: i1=intItem {retval.add($i1.retval);} (COMMA i2=intItem {retval.add($i2.retval);})*;
	
strItemList returns [List<Object> retval]
@init {retval = Lists.newArrayList();}
	: i1=strItem {retval.add($i1.retval);} (COMMA i2=strItem {retval.add($i2.retval);})*;
	
intItem returns [ValueExpr retval]
	: n=numericExpr					{retval = $n.retval;};

strItem returns [ValueExpr retval]
	: s=stringExpr					{$strItem.retval = $s.retval;};

qstringList returns [List<String> retval]
@init {retval = Lists.newArrayList();}
	: qstring[retval] (COMMA qstring[retval])*;

column [List<String> list]	
	: charstr=dottedValue 				{if (list != null) list.add($charstr.text);};

dottedValue	
	: ID ((DOT | COLON) ID)*;

qstring	[List<String> list]
	: QUOTED 					{if (list != null) list.add($QUOTED.text);};

plusMinus returns [CalcExpr.OP retval]
	: PLUS						{retval = CalcExpr.OP.PLUS;}
	| MINUS						{retval = CalcExpr.OP.MINUS;}
	;
	
multDiv returns [CalcExpr.OP retval]
	: STAR						{retval = CalcExpr.OP.MULT;}
	| DIV						{retval = CalcExpr.OP.DIV;}
	;
	
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

keySELECT 	: {isKeyword(input, "SELECT")}? ID;
keyDELETE 	: {isKeyword(input, "DELETE")}? ID;
keyCREATE 	: {isKeyword(input, "CREATE")}? ID;
keyDESCRIBE 	: {isKeyword(input, "DESCRIBE")}? ID;
keySHOW 	: {isKeyword(input, "SHOW")}? ID;
keyTABLE 	: {isKeyword(input, "TABLE")}? ID;
keyTABLES 	: {isKeyword(input, "TABLES")}? ID;
keyWHERE	: {isKeyword(input, "WHERE")}? ID;
keyFROM 	: {isKeyword(input, "FROM")}? ID;
keySET 		: {isKeyword(input, "SET")}? ID;
keyIN 		: {isKeyword(input, "IN")}? ID;
keyIS 		: {isKeyword(input, "IS")}? ID;
keyLIKE		: {isKeyword(input, "LIKE")}? ID;
keyTO 		: {isKeyword(input, "TO")}? ID;
keyOR 		: {isKeyword(input, "OR")}? ID;
keyAND 		: {isKeyword(input, "AND")}? ID;
keyNOT 		: {isKeyword(input, "NOT")}? ID;
keyTRUE 	: {isKeyword(input, "TRUE")}? ID;
keyFALSE 	: {isKeyword(input, "FALSE")}? ID;
keyBETWEEN 	: {isKeyword(input, "BETWEEN")}? ID;
keyNULL 	: {isKeyword(input, "NULL")}? ID;
keyLOWER 	: {isKeyword(input, "LOWER")}? ID;
keyUPPER 	: {isKeyword(input, "UPPER")}? ID;
keyTRIM 	: {isKeyword(input, "TRIM")}? ID;
keyCONCAT 	: {isKeyword(input, "CONCAT")}? ID;
keySUBSTRING 	: {isKeyword(input, "SUBSTRING")}? ID;
keyLENGTH 	: {isKeyword(input, "LENGTH")}? ID;
keyABS 		: {isKeyword(input, "ABS")}? ID;
keyMOD	 	: {isKeyword(input, "MOD")}? ID;
keyCURRENT_DATE	: {isKeyword(input, "CURRENT_DATE")}? ID;
keyCURRENT_TIME : {isKeyword(input, "CURRENT_TIME")}? ID;
keyCURRENT_TIMESTAMP : {isKeyword(input, "CURRENT_TIMESTAMP")}? ID;
