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
	NE = '!=';	
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
		
orExpr returns [Predicate retval]
	: expr1=andExpr (keyOR expr2=orExpr)?		{retval= new OrExpr($expr1.retval, $expr2.retval);;}
	//| cond_expr keyOR cond_term
	;

andExpr returns [Predicate retval]
	: expr1=condFactor (keyAND expr2=andExpr)?	{retval = new AndExpr($expr1.retval, $expr2.retval);}
	//| cond_term keyAND cond_factor
	;
	
condFactor returns [Predicate retval]			 
	: k=keyNOT? p=condPrimary			{retval = new CondFactor(($k.text != null), $p.retval);}
	;

condPrimary returns [Predicate retval]
	: simpleCondExpr  				{retval = $simpleCondExpr.retval;}
	| LPAREN orExpr RPAREN				{retval = $orExpr.retval;}
	;

simpleCondExpr returns [Predicate retval]
	: betweenExpr					{retval = $betweenExpr.retval;}
	| likeExpr					//{retval = $likeExpr.retval;}
	| inExpr					{retval = $inExpr.retval;}
	| nullCompExpr					//{retval = $nullCompExpr.retval;}
	| compareExpr 					{retval = $compareExpr.retval;}
	| booleanExpr					{retval = $booleanExpr.retval;}
	;

betweenExpr returns [Predicate retval]
	: a=attribRef[ExprType.NumberType] n=keyNOT? 
	  keyBETWEEN n1=numberExpr keyAND n2=numberExpr	{retval = new Between(ExprType.NumberType, $a.retval, ($n.text != null), $n1.retval, $n2.retval);}
	| a=attribRef[ExprType.StringType] n=keyNOT?  
	  keyBETWEEN s1=stringExpr keyAND s2=stringExpr	{retval = new Between(ExprType.StringType, $a.retval, ($n.text != null), $n1.retval, $n2.retval);}
	| a=attribRef[ExprType.DateType] n=keyNOT? 
	  keyBETWEEN d1=dateExpr keyAND d2=dateExpr
	;

likeExpr 
	: attribRef[ExprType.StringType] 
	  keyNOT? keyLIKE pattern_value=stringLiteral; // ('ESCAPE' escape_character=string_literal)?;

inExpr returns [Predicate retval]
	: a=attribRef[ExprType.NumberType] n=keyNOT? keyIN 
	  LPAREN i=intItemList RPAREN			{retval = new In(ExprType.NumberType, $a.retval, ($n.text != null), $i.retval);} 
	| a=attribRef[ExprType.StringType] n=keyNOT? keyIN 
	  LPAREN s=strItemList RPAREN			{retval = new In(ExprType.StringType, $a.retval, ($n.text != null), $s.retval);} 
	;

intItem returns [Value retval]
	: n=numberLiteral				{retval = $n.retval;};

strItem : stringLiteral;

nullCompExpr
	: attribRef[ExprType.StringType] keyIS (keyNOT)? keyNULL;

compareExpr returns [Predicate retval]
	: a=attribRef[ExprType.StringType] o=compOp s=stringExpr	{retval = new StringCompare($a.retval, $o.retval, $s.retval);}
	| a=attribRef[ExprType.DateType] o=compOp dateExpr 
	| a=attribRef[ExprType.NumberType] o=compOp n=numberExpr	{retval = new NumberCompare($a.retval, $o.retval, $n.retval);}
	| s=stringExpr o=compOp a=attribRef[ExprType.StringType]	{retval = new StringCompare($s.retval, $o.retval, $a.retval);}
	| dateExpr o=compOp a=attribRef[ExprType.DateType]
	| n=numberExpr o=compOp a=attribRef[ExprType.NumberType]	{retval = new NumberCompare($n.retval, $o.retval, $a.retval);}
	| n1=numberExpr o=compOp n2=numberExpr				{retval = new NumberCompare($n1.retval, $o.retval, $n2.retval);}
	;
	
compOp returns [CompareExpr.Operator retval]
	: EQ EQ?					{retval = CompareExpr.Operator.EQ;}
	| GT 						{retval = CompareExpr.Operator.GT;}
	| GTEQ 						{retval = CompareExpr.Operator.GTEQ;}
	| LT 						{retval = CompareExpr.Operator.LT;}
	| LTEQ 						{retval = CompareExpr.Operator.LTEQ;}
	| (LTGT | NE)					{retval = CompareExpr.Operator.LTGT;}
	;

numberExpr returns [Value retval]
	: n=simpleNumberExpr				{retval = $n.retval;}	
	;

simpleNumberExpr returns [Value retval]
	: n1=numberTerm (op=plusMinus n2=simpleNumberExpr)?
	{$simpleNumberExpr.retval= ($n2.text == null) ? new CalcExpr($n1.retval) : new CalcExpr($n1.retval, $op.retval, $n2.retval);}
	//| simpleNumberExpr plusMinus numberTerm
	;

numberTerm returns [Value retval]
	: n1=numberFactor (op=multDiv n2=numberTerm)?
	{$numberTerm.retval = ($n2.text == null) ? new CalcExpr($n1.retval) : new CalcExpr($n1.retval, $op.retval, $n2.retval);}
	//| numberTerm multDiv numberFactor
	;

plusMinus returns [CalcExpr.OP retval]
	: PLUS						{retval = CalcExpr.OP.PLUS;}
	| MINUS						{retval = CalcExpr.OP.MINUS;}
	;
	
multDiv returns [CalcExpr.OP retval]
	: STAR						{retval = CalcExpr.OP.MULT;}
	| DIV						{retval = CalcExpr.OP.DIV;}
	;
	
numberFactor returns [Value retval]
	: (s=plusMinus)? n=numberPrimary 		
	{retval = ($s.retval == CalcExpr.OP.MINUS) ? new CalcExpr($n.retval, CalcExpr.OP.NEGATIVE, null) : $n.retval;}
	;
	
numberPrimary returns [Value retval]
	: n=numberLiteral				{retval = $n.retval;}
	| LPAREN s=simpleNumberExpr RPAREN		{retval = $s.retval;}
	| funcsReturningNumber
	;

stringExpr returns [Value retval]
	: s=stringLiteral				{retval = $s.retval;}
	| f=funcReturningString
	| a=attribRef[ExprType.StringType]		{retval = $a.retval;}
	;

booleanExpr returns [Predicate retval]
	: b=booleanLiteral				{retval = new BooleanExpr($b.retval);}
	| func=funcReturningBoolean
	;

dateExpr
	: funcReturningDatetime
	;

funcsReturningNumber
	: keyLENGTH LPAREN stringExpr RPAREN
	| keyABS LPAREN simpleNumberExpr RPAREN
	| keyMOD LPAREN simpleNumberExpr COMMA simpleNumberExpr RPAREN
	;

funcReturningDatetime
	: keyCURRENT_DATE
	| keyCURRENT_TIME
	| keyCURRENT_TIMESTAMP
	;

funcReturningString
	: keyCONCAT LPAREN stringExpr COMMA stringExpr RPAREN
	| keySUBSTRING LPAREN stringExpr COMMA simpleNumberExpr COMMA simpleNumberExpr RPAREN
	| keyTRIM LPAREN stringExpr RPAREN
	| keyLOWER LPAREN stringExpr RPAREN
	| keyUPPER LPAREN stringExpr RPAREN
	;
	
funcReturningBoolean
	: 
	;

attribRef [ExprType type] returns [AttribRef retval]
	: v=ID 						{retval = new AttribRef(type, $v.text);};
		
stringLiteral returns [StringLiteral retval]
	: v=QUOTED 					{retval = new StringLiteral($v.text);};
	
numberLiteral returns [NumberLiteral retval]
	: v=INT						{retval = new NumberLiteral(Integer.valueOf($v.text));};
		
booleanLiteral returns [BooleanLiteral retval]
	: t=keyTRUE					{retval = new BooleanLiteral($t.text);}
	| f=keyFALSE					{retval = new BooleanLiteral($f.text);}
	;
		
intItemList returns [List<Object> retval]
@init {retval = Lists.newArrayList();}
	: item1=intItem {retval.add($item1.retval);} (COMMA item2=intItem {retval.add($item2.retval);})*;
	
strItemList returns [List<Object> retval]
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

keySELECT 	: {HUtil.isKeyword(input, "SELECT")}? ID;
keyDELETE 	: {HUtil.isKeyword(input, "DELETE")}? ID;
keyCREATE 	: {HUtil.isKeyword(input, "CREATE")}? ID;
keyDESCRIBE 	: {HUtil.isKeyword(input, "DESCRIBE")}? ID;
keySHOW 	: {HUtil.isKeyword(input, "SHOW")}? ID;
keyTABLE 	: {HUtil.isKeyword(input, "TABLE")}? ID;
keyTABLES 	: {HUtil.isKeyword(input, "TABLES")}? ID;
keyWHERE	: {HUtil.isKeyword(input, "WHERE")}? ID;
keyFROM 	: {HUtil.isKeyword(input, "FROM")}? ID;
keySET 		: {HUtil.isKeyword(input, "SET")}? ID;
keyIN 		: {HUtil.isKeyword(input, "IN")}? ID;
keyIS 		: {HUtil.isKeyword(input, "IS")}? ID;
keyLIKE		: {HUtil.isKeyword(input, "LIKE")}? ID;
keyTO 		: {HUtil.isKeyword(input, "TO")}? ID;
keyOR 		: {HUtil.isKeyword(input, "OR")}? ID;
keyAND 		: {HUtil.isKeyword(input, "AND")}? ID;
keyNOT 		: {HUtil.isKeyword(input, "NOT")}? ID;
keyTRUE 	: {HUtil.isKeyword(input, "TRUE")}? ID;
keyFALSE 	: {HUtil.isKeyword(input, "FALSE")}? ID;
keyBETWEEN 	: {HUtil.isKeyword(input, "BETWEEN")}? ID;
keyNULL 	: {HUtil.isKeyword(input, "NULL")}? ID;
keyLOWER 	: {HUtil.isKeyword(input, "LOWER")}? ID;
keyUPPER 	: {HUtil.isKeyword(input, "UPPER")}? ID;
keyTRIM 	: {HUtil.isKeyword(input, "TRIM")}? ID;
keyCONCAT 	: {HUtil.isKeyword(input, "CONCAT")}? ID;
keySUBSTRING 	: {HUtil.isKeyword(input, "SUBSTRING")}? ID;
keyLENGTH 	: {HUtil.isKeyword(input, "LENGTH")}? ID;
keyABS 		: {HUtil.isKeyword(input, "ABS")}? ID;
keyMOD	 	: {HUtil.isKeyword(input, "MOD")}? ID;
keyCURRENT_DATE	: {HUtil.isKeyword(input, "CURRENT_DATE")}? ID;
keyCURRENT_TIME : {HUtil.isKeyword(input, "CURRENT_TIME")}? ID;
keyCURRENT_TIMESTAMP : {HUtil.isKeyword(input, "CURRENT_TIMESTAMP")}? ID;
