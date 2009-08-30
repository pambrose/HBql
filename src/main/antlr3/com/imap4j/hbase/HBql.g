grammar HBql;

options {
	superClass=HBaseParser;
	backtrack=true;
}

tokens {
	DOT = '.';
	COLON = ':';
	QMARK = '?';
	STAR = '*';
	DIV = '/';
	COMMA = ',';
	PLUS = '+';
	MINUS = '-';
	MOD = '%';
	NOT = '!';
	OR = '||';
	AND = '&&';
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
	LBRACE = '[';
	RBRACE = ']';
}

@rulecatch {catch (RecognitionException re) {handleRecognitionException(re);}}

@header {
package com.imap4j.hbase;
import com.imap4j.hbase.hbql.*;
import com.imap4j.hbase.hbql.expr.*;
import com.imap4j.hbase.hbql.expr.predicate.*;
import com.imap4j.hbase.hbql.expr.value.*;
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
	: keySET i=ID to? v=dottedValue 		{retval = new SetArgs($i.text, $v.text);};

whereClause returns [WhereExpr retval]
	: keyWHERE e=orExpr 				{retval = new WhereExpr($e.retval);};
		
orExpr returns [PredicateExpr retval]
	: e1=andExpr (or e2=orExpr)?			{$orExpr.retval = ($e2.text == null) ? $e1.retval : new BooleanExpr($e1.retval, BooleanExpr.OP.OR, $e2.retval);};

andExpr returns [PredicateExpr retval]
	: e1=condFactor (and e2=andExpr)?		{$andExpr.retval = ($e2.text == null) ? $e1.retval : new BooleanExpr($e1.retval, BooleanExpr.OP.AND, $e2.retval);};

condFactor returns [PredicateExpr retval]			 
	: n=not? p=condPrimary				{$condFactor.retval = ($n.text != null) ?  new CondFactor(true, $p.retval) :  $p.retval;};
	
condPrimary returns [PredicateExpr retval]
	: s=simpleCondExpr  				{retval = $s.retval;}
	| LPAREN o=orExpr RPAREN			{retval = $o.retval;}
	;

simpleCondExpr returns [PredicateExpr retval]
	: b1=betweenStmt				{retval = $b1.retval;}
	| l=likeStmt					{retval = $l.retval;}
	| i=inStmt					{retval = $i.retval;}
	| b2=booleanStmt				{retval = $b2.retval;}
	| n=nullCompExpr				{retval = $n.retval;}
	| c=compareExpr 				{retval = $c.retval;}
	;

betweenStmt returns [PredicateExpr retval]
	: n1=numericExpr n=not? keyBETWEEN n2=numericExpr and n3=numericExpr		
							{retval = new NumberBetweenStmt($n1.retval, ($n.text != null), $n2.retval, $n3.retval);}
	| s1=stringExpr n=not? keyBETWEEN s2=stringExpr and s3=stringExpr		
							{retval = new StringBetweenStmt($s1.retval, ($n.text != null), $s2.retval, $s3.retval);}
	//| d1=dateExpr n=not? keyBETWEEN d2=dateExpr and d3=dateExpr
	;

likeStmt returns [PredicateExpr retval]
	: stringExpr keyNOT? keyLIKE p=stringExpr; // ('ESCAPE' escape_character=string_literal)?;

inStmt returns [PredicateExpr retval]
	: a1=numericExpr n=not? keyIN LPAREN i=intItemList RPAREN			
							{retval = new NumberInStmt($a1.retval,($n.text != null), $i.retval);} 
	| a2=stringExpr n=not? keyIN LPAREN s=strItemList RPAREN			
							{retval = new StringInStmt($a2.retval, ($n.text != null), $s.retval);} 
	;

booleanStmt returns [PredicateExpr retval]
	: b=booleanExpr					{retval = new BooleanStmt($b.retval);};
	
nullCompExpr returns [PredicateExpr retval]
	: a=stringExpr keyIS (n=keyNOT)? keyNULL	{retval = new NullCompare(($n.text != null), $a.retval);};	

compareExpr returns [PredicateExpr retval]
	: s1=stringExpr o=compOp s2=stringExpr	  	{retval = new StringCompare($s1.retval, $o.retval, $s2.retval);}
	//| d1=dateExpr o=compOp d2=dateExpr 
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
numericExpr returns [NumberValue retval]
	: m=multdivExpr (op=plusMinus n=numericExpr)?	{$numericExpr.retval= ($n.text == null) ? $m.retval : new CalcExpr($m.retval, $op.retval, $n.retval);}
	;

multdivExpr returns [NumberValue retval]
	: c=calcNumberExpr (op=multDiv m=multdivExpr)?	{$multdivExpr.retval = ($m.text == null) ? $c.retval : new CalcExpr($c.retval, $op.retval, $m.retval);}
	;

calcNumberExpr returns [NumberValue retval]
	: (s=plusMinus)? n=numPrimary 			{retval = ($s.retval == CalcExpr.OP.MINUS) ? new CalcExpr($n.retval, CalcExpr.OP.NEGATIVE, null) :  $n.retval;}
	;

numPrimary returns [NumberValue retval]
	: n=numberExpr					{retval = $n.retval;}
	| LPAREN s=numericExpr RPAREN			{retval = $s.retval;}
	;
	   						 
// Simple typed exprs
numberExpr returns [NumberValue retval]
	: l=numberLiteral				{retval = $l.retval;} 
	| i=intAttrib					{retval = $i.retval;}
	//| f=funcReturningNumber
	| LBRACE e=orExpr QMARK n1=numericExpr COLON n2=numericExpr RBRACE	
							{retval = new Ternary($e.retval, $n1.retval, $n2.retval);}
	;

// Supports string concatenation -- avoids creating a list everytime
stringExpr returns [StringValue retval]
@init {
  StringValue firstval = null;  
  List<StringValue> vals = null;
} 
	: s1=stringVal {firstval = $s1.retval;} 
	  (PLUS s2=stringExpr {if (vals == null) {
	    			 vals = Lists.newArrayList();
	    			 vals.add(firstval);
	    		       } 
	    		       vals.add($s2.retval);
	    		      })*
							{retval = (vals == null) ? firstval : new StringConcat(vals);}
	;
	
stringVal returns [StringValue retval]
	: s=stringLiteral				{retval = $s.retval;}
	| f=funcReturningString				{retval = $f.retval;}
	| n=keyNULL					{retval = new NullLiteral();}
	| a=strAttrib					{retval = $a.retval;}
	| LBRACE e=orExpr QMARK s1=stringExpr COLON s2=stringExpr RBRACE	
							{retval = new StringTernary($e.retval, $s1.retval, $s2.retval);}
	;

booleanExpr returns [BooleanValue retval]
	: b=booleanLiteral				{retval = $b.retval;}
	| LBRACE e=orExpr QMARK b1=booleanExpr COLON b2=booleanExpr RBRACE	
							{retval = new BooleanTernary($e.retval, $b1.retval, $b2.retval);}
	//| f=funcReturningBoolean
	;
/*
dateExpr returns [DateValue retval]
	: funcReturningDatetime
	;
*/

// Attribs with type
strAttrib returns [StringValue retval]
	: v=ID 						{retval = new StringAttribRef($v.text);};

intAttrib returns [NumberValue retval]
	: v=ID 						{retval = new NumberAttribRef($v.text);};

dateAttrib returns [DateValue retval]
	: v=ID 						{retval = new DateAttribRef($v.text);};

// Literals		
stringLiteral returns [StringValue retval]
	: v=QUOTED 					{retval = new StringLiteral($v.text);};
	
numberLiteral returns [NumberValue retval]
	: v=INT						{retval = new NumberLiteral(Integer.valueOf($v.text));};
		
booleanLiteral returns [BooleanValue retval]
	: t=keyTRUE					{retval = new BooleanLiteral($t.text);}
	| f=keyFALSE					{retval = new BooleanLiteral($f.text);}
	;

/*
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
*/

funcReturningString returns [StringValue retval]
	: keyCONCAT LPAREN s1=stringExpr COMMA s2=stringExpr RPAREN
							{retval = new StringFunction(StringFunction.FUNC.CONCAT, $s1.retval, $s2.retval);}
	| keySUBSTRING LPAREN s=stringExpr COMMA n1=numericExpr COMMA n2=numericExpr RPAREN
							{retval = new Substring($s.retval, $n1.retval, $n2.retval);}
	| keyTRIM LPAREN s=stringExpr RPAREN		{retval = new StringFunction(StringFunction.FUNC.TRIM, $s.retval);}
	| keyLOWER LPAREN s=stringExpr RPAREN		{retval = new StringFunction(StringFunction.FUNC.LOWER, $s.retval);} 
	| keyUPPER LPAREN s=stringExpr RPAREN		{retval = new StringFunction(StringFunction.FUNC.UPPER, $s.retval);} 
	;
/*	
funcReturningBoolean
	: 
	;
*/
		
intItemList returns [List<Object> retval]
@init {retval = Lists.newArrayList();}
	: i1=intItem {retval.add($i1.retval);} (COMMA i2=intItem {retval.add($i2.retval);})*;
	
strItemList returns [List<StringValue> retval]
@init {retval = Lists.newArrayList();}
	: i1=strItem {retval.add($i1.retval);} (COMMA i2=strItem {retval.add($i2.retval);})*;
	
intItem returns [NumberValue retval]
	: n=numericExpr					{retval = $n.retval;};

strItem returns [StringValue retval]
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
	| MOD						{retval = CalcExpr.OP.MOD;}
	;

to 	: keyTO | EQ;
or	: keyOR | OR;
and	: keyAND | AND;
not	: keyNOT | NOT;
		
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
//keyLENGTH 	: {isKeyword(input, "LENGTH")}? ID;
//keyABS 	: {isKeyword(input, "ABS")}? ID;
//keyMOD	 : {isKeyword(input, "MOD")}? ID;
//keyCURRENT_DATE: {isKeyword(input, "CURRENT_DATE")}? ID;
//keyCURRENT_TIME: {isKeyword(input, "CURRENT_TIME")}? ID;
//keyCURRENT_TIMESTAMP : {isKeyword(input, "CURRENT_TIMESTAMP")}? ID;
