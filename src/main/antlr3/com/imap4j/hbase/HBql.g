grammar HBql;

options {superClass=HBaseParser;}

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
import com.imap4j.hbase.hbql.expr.node.*;
import com.imap4j.hbase.hbql.expr.predicate.*;
import com.imap4j.hbase.hbql.expr.value.func.*;
import com.imap4j.hbase.hbql.expr.value.literal.*;
import com.imap4j.hbase.hbql.expr.value.var.*;
import com.imap4j.hbase.antlr.args.*;
import com.imap4j.hbase.antlr.*;
import com.imap4j.hbase.hbql.schema.*;
import java.util.Date;
import com.google.common.collect.Lists;
}

@lexer::header {
package com.imap4j.hbase;
import com.google.common.collect.Lists;
}

selectStmt [ClassSchema cs] returns [QueryArgs retval]
	: keySELECT (STAR | c=columnList) keyFROM t=ID w=whereValue[cs]?			
							{retval = new QueryArgs($c.retval, $t.text, $w.retval);};

columnList returns [List<String> retval]
@init {retval = Lists.newArrayList();}
	: column[retval] (COMMA column[retval])*;

execCommand [ClassSchema cs] returns [ExecArgs retval]
	: create=createStmt				{retval = $create.retval;}
	| desc=describeStmt 				{retval = $desc.retval;}
	| show=showStmt 				{retval = $show.retval;}
	| del=deleteStmt[cs] 				{retval = $del.retval;}
	| set=setStmt					{retval = $set.retval;}
	;

createStmt returns [CreateArgs retval]
	: keyCREATE keyTABLE t=ID 			{retval = new CreateArgs($t.text);};

describeStmt returns [DescribeArgs retval]
	: keyDESCRIBE keyTABLE t=ID 		{retval = new DescribeArgs($t.text);};

showStmt returns [ShowArgs retval]
	: keySHOW keyTABLES 		 		{retval = new ShowArgs();};

deleteStmt [ClassSchema cs] returns [DeleteArgs retval]
	: keyDELETE keyFROM t=ID 			{setClassSchema($t.text);}
	  w=whereValue[cs]?				{retval = new DeleteArgs($t.text, $w.retval);};

setStmt returns [SetArgs retval]
	: keySET i=ID to? v=QUOTED	 		{retval = new SetArgs($i.text, $v.text);};

whereValue [ClassSchema cs] returns [WhereArgs retval]
@init {retval = new WhereArgs();}
	: keyWITH
	  k=keys?					{retval.setKeyRangeArgs($k.retval);}
	  t=time?					{retval.setDateRangeArgs($t.retval);}	
	  v=versions?					{retval.setVersionArgs($v.retval);}
	  c=clientFilter[cs]?				{retval.setClientFilterArgs($c.retval);}
	  s=serverFilter[cs]?				{retval.setServerFilterArgs($s.retval);}
	;

keys returns [KeyRangeArgs retval]
	: keyKEYS k=keyRangeList			{retval = new KeyRangeArgs($k.retval);}	
	;
	
time returns [DateRangeArgs retval]
	: keyTIME keyRANGE? 
	  d1=rangeDateExpr COLON d2=rangeDateExpr	{retval = new DateRangeArgs($d1.retval, $d2.retval);};
		
versions returns [VersionArgs retval]
	: keyVERSIONS v=integerLiteral			{retval = new VersionArgs($v.retval);};
	
serverFilter [ClassSchema cs] returns [ExprEvalTree retval]
	: keySERVER keyFILTER? w=whereExpr[cs]	{retval = $w.retval;};
	
clientFilter [ClassSchema cs] returns [ExprEvalTree retval]
	: keyCLIENT keyFILTER? w=whereExpr[cs]	{retval = $w.retval;};
	
keyRangeList returns [List<KeyRangeArgs.Range> retval]
@init {retval = Lists.newArrayList();}
	:  k1=keyRange {retval.add($k1.retval);} (COMMA k2=keyRange {retval.add($k2.retval);})*
	;
	
keyRange returns [KeyRangeArgs.Range retval]
	: q=QUOTED					{retval = new KeyRangeArgs.Range($q.text);}
	| q1=QUOTED COLON q2=QUOTED			{retval = new KeyRangeArgs.Range($q1.text, $q2.text);}
	;
		
filterClause [ClassSchema cs] returns [ExprEvalTree retval]
	: keyWITH keyFILTER w=whereExpr[cs]		{retval = $w.retval;};
	
whereClause [ClassSchema cs] returns [ExprEvalTree retval]
	: keyWHERE w=whereExpr[cs] 			{retval = $w.retval;};

whereExpr [ClassSchema cs] returns [ExprEvalTree retval]
@init {setClassSchema(cs);}
	: e=orExpr					{retval = new ExprEvalTree($e.retval);};
			
orExpr returns [PredicateExpr retval]
	: e1=andExpr (or e2=orExpr)?			{$orExpr.retval = ($e2.text == null) ? $e1.retval : new BooleanExpr($e1.retval, BooleanExpr.OP.OR, $e2.retval);};

andExpr returns [PredicateExpr retval]
	: e1=condFactor (and e2=andExpr)?		{$andExpr.retval = ($e2.text == null) ? $e1.retval : new BooleanExpr($e1.retval, BooleanExpr.OP.AND, $e2.retval);};

condFactor returns [PredicateExpr retval]			 
	: n=not? p=condPrimary				{$condFactor.retval = ($n.text != null) ?  new CondFactor(true, $p.retval) :  $p.retval;};
	
condPrimary returns [PredicateExpr retval]
options {backtrack=true;}	
	: s=simpleCondExpr  				{retval = $s.retval;}
	| LPAREN o=orExpr RPAREN			{retval = $o.retval;}
	;

simpleCondExpr returns [PredicateExpr retval]
options {backtrack=true;}	
	: n=nullCompExpr				{retval = $n.retval;}
	| c=compareExpr 				{retval = $c.retval;}
	| b1=betweenStmt				{retval = $b1.retval;}
	| l=likeStmt					{retval = $l.retval;}
	| i=inStmt					{retval = $i.retval;}
	| b2=booleanStmt				{retval = $b2.retval;}
	;

betweenStmt returns [PredicateExpr retval]
options {backtrack=true;}	
	: d1=dateExpr n=not? keyBETWEEN d2=dateExpr and d3=dateExpr
							{retval = new DateBetweenStmt($d1.retval, ($n.text != null), $d2.retval, $d3.retval);}
	| n1=numericExpr n=not? keyBETWEEN n2=numericExpr and n3=numericExpr		
							{retval = new IntegerBetweenStmt($n1.retval, ($n.text != null), $n2.retval, $n3.retval);}
	| s1=stringExpr n=not? keyBETWEEN s2=stringExpr and s3=stringExpr		
							{retval = new StringBetweenStmt($s1.retval, ($n.text != null), $s2.retval, $s3.retval);}
	;

likeStmt returns [PredicateExpr retval]
	: s1=stringExpr n=not? keyLIKE s2=stringExpr 
							{retval = new LikeStmt($s1.retval, ($n.text != null), $s2.retval);};

inStmt returns [PredicateExpr retval]
options {backtrack=true;}	
	: a3=dateExpr n=not? keyIN LPAREN d=dateItemList RPAREN			
							{retval = new DateInStmt($a3.retval, ($n.text != null), $d.retval);} 
	| a1=numericExpr n=not? keyIN LPAREN i=intItemList RPAREN			
							{retval = new IntegerInStmt($a1.retval,($n.text != null), $i.retval);} 
	| a2=stringExpr n=not? keyIN LPAREN s=strItemList RPAREN			
							{retval = new StringInStmt($a2.retval, ($n.text != null), $s.retval);} 
	;

booleanStmt returns [PredicateExpr retval]
	: b=booleanExpr					{retval = new BooleanStmt($b.retval);};
	
nullCompExpr returns [PredicateExpr retval]
	: s=stringExpr keyIS (n=keyNOT)? keyNULL	{retval = new StringNullCompare(($n.text != null), $s.retval);}	
	// | d=dateExpr keyIS (n=keyNOT)? keyNULL	{retval = new DateNullCompare(($n.text != null), $d.retval);}
	;	

compareExpr returns [PredicateExpr retval]
options {backtrack=true;}	
	: d1=dateExpr o=compOp d2=dateExpr 		{retval = new DateCompare($d1.retval, $o.retval, $d2.retval);}	
	| s1=stringExpr o=compOp s2=stringExpr	  	{retval = new StringCompare($s1.retval, $o.retval, $s2.retval);}
	| n1=numericExpr o=compOp n2=numericExpr	{retval = new IntegerCompare($n1.retval, $o.retval, $n2.retval);}
	;
	
compOp returns [CompareExpr.OP retval]
	: EQ EQ?					{retval = CompareExpr.OP.EQ;}
	| GT 						{retval = CompareExpr.OP.GT;}
	| GTEQ 						{retval = CompareExpr.OP.GTEQ;}
	| LT 						{retval = CompareExpr.OP.LT;}
	| LTEQ 						{retval = CompareExpr.OP.LTEQ;}
	| (LTGT | BANGEQ)				{retval = CompareExpr.OP.NOTEQ;}
	;

// Numeric calculations
numericExpr returns [IntegerValue retval]
	: m=multdivExpr (op=plusMinus n=numericExpr)?	{$numericExpr.retval= ($n.text == null) ? $m.retval : new CalcExpr($m.retval, $op.retval, $n.retval);}
	;

multdivExpr returns [IntegerValue retval]
	: c=calcNumberExpr (op=multDiv m=multdivExpr)?	{$multdivExpr.retval = ($m.text == null) ? $c.retval : new CalcExpr($c.retval, $op.retval, $m.retval);}
	;

calcNumberExpr returns [IntegerValue retval]
	: (s=plusMinus)? n=numPrimary 			{retval = ($s.retval == CalcExpr.OP.MINUS) ? new CalcExpr($n.retval, CalcExpr.OP.NEGATIVE, null) :  $n.retval;}
	;

numPrimary returns [IntegerValue retval]
	: n=integerExpr					{retval = $n.retval;}
	| LPAREN s=numericExpr RPAREN			{retval = $s.retval;}
	;
	   						 
// Simple typed exprs
integerExpr returns [IntegerValue retval]
options {backtrack=true;}	
	: l=integerVal					{retval = $l.retval;} 
	| LBRACE keyIF e=orExpr keyTHEN n1=numericExpr keyELSE n2=numericExpr RBRACE 	
							{retval = new IntegerTernary($e.retval, $n1.retval, $n2.retval);}
	;

integerVal returns [IntegerValue retval]
	: l=integerLiteral				{retval = $l.retval;} 
	| i=attribVar					{retval = (IntegerValue)$i.retval;}
	//| f=funcReturningInteger
	;

// Supports string concatenation -- avoids creating a list everytime
stringExpr returns [StringValue retval]
@init {
  StringValue firstval = null;  
  List<StringValue> vals = null;
} 
	: s1=stringParen {firstval = $s1.retval;} 
	  (PLUS s2=stringExpr {if (vals == null) {
	    			 vals = Lists.newArrayList();
	    			 vals.add(firstval);
	    		       } 
	    		       vals.add($s2.retval);
	    		      })?
							{retval = (vals == null) ? firstval : new StringConcat(vals);}
	;

stringParen returns [StringValue retval]
	: s1=stringVal					{retval = $s1.retval;}
	| LPAREN s2=stringExpr	RPAREN			{retval = $s2.retval;}						
	;
		
stringVal returns [StringValue retval]
options {backtrack=true;}	
	: sl=stringLiteral				{retval = $sl.retval;}
	| f=funcReturningString				{retval = $f.retval;}
	| n=keyNULL					{retval = new StringNullLiteral();}
	| a=attribVar					{retval = (StringValue)$a.retval;}
	| LBRACE keyIF e=orExpr keyTHEN s1=stringExpr keyELSE s2=stringExpr RBRACE 	
							{retval = new StringTernary($e.retval, $s1.retval, $s2.retval);}
	;

booleanExpr returns [BooleanValue retval]
options {backtrack=true;}	
	: b=booleanVal					{retval = $b.retval;}
	| LPAREN e=orExpr RPAREN			{retval = new BooleanPredicate($e.retval);}
	;

booleanVal returns [BooleanValue retval]
options {backtrack=true;}	
	: b=booleanLiteral				{retval = $b.retval;}
	//| f=funcReturningBoolean
	| LBRACE keyIF e=orExpr keyTHEN b1=orExpr keyELSE b2=orExpr RBRACE	
							{retval = new BooleanTernary($e.retval, $b1.retval, $b2.retval);}
	;

rangeDateExpr returns [DateValue retval]
	: d1=rangeDateVal					{retval = $d1.retval;}
	| LPAREN d2=rangeDateExpr RPAREN			{retval = $d2.retval;}
	;

rangeDateVal returns [DateValue retval]
	: d1=dateLiteral				{retval = $d1.retval;}
	| d2=funcReturningDatetime			{retval = $d2.retval;}
	;
	
dateExpr returns [DateValue retval]
	: d1=dateVal					{retval = $d1.retval;}
	| LPAREN d2=dateExpr RPAREN			{retval = $d2.retval;}
	;
	
dateVal returns [DateValue retval]
	: d1=dateLiteral				{retval = $d1.retval;}
	| d2=funcReturningDatetime			{retval = $d2.retval;}
	| d3=attribVar					{retval = (DateValue)$d3.retval;} 			
	;

// Generic Attrib
attribVar returns [ValueExpr retval]
	: v=varRef 					{retval = this.getValueExpr($v.text);};

// Literals		
stringLiteral returns [StringValue retval]
	: v=QUOTED 					{retval = new StringLiteral($v.text);};
	
integerLiteral returns [IntegerValue retval]
	: v=INT						{retval = new IntegerLiteral(Integer.valueOf($v.text));};
		
dateLiteral returns [DateValue retval]
	: keyNOW					{retval = new DateLiteral(DateLiteral.TYPE.TODAY);}
	| keyYESTERDAY					{retval = new DateLiteral(DateLiteral.TYPE.YESTERDAY);}
	| keyTOMORROW					{retval = new DateLiteral(DateLiteral.TYPE.TOMORROW);}
	;

booleanLiteral returns [BooleanValue retval]
	: t=keyTRUE					{retval = new BooleanLiteral($t.text);}
	| f=keyFALSE					{retval = new BooleanLiteral($f.text);}
	;

// Functions
funcReturningDatetime returns [DateValue retval]
	: keyDATE LPAREN s1=stringExpr COMMA s2=stringExpr RPAREN
							{retval = new DateExpr($s1.retval, $s2.retval);}
	;

funcReturningString returns [StringValue retval]
	: keyCONCAT LPAREN s1=stringExpr COMMA s2=stringExpr RPAREN
							{retval = new StringFunction(GenericFunction.FUNC.CONCAT, $s1.retval, $s2.retval);}
	| keySUBSTRING LPAREN s=stringExpr COMMA n1=numericExpr COMMA n2=numericExpr RPAREN
							{retval = new Substring($s.retval, $n1.retval, $n2.retval);}
	| keyTRIM LPAREN s=stringExpr RPAREN		{retval = new StringFunction(GenericFunction.FUNC.TRIM, $s.retval);}
	| keyLOWER LPAREN s=stringExpr RPAREN		{retval = new StringFunction(GenericFunction.FUNC.LOWER, $s.retval);} 
	| keyUPPER LPAREN s=stringExpr RPAREN		{retval = new StringFunction(GenericFunction.FUNC.UPPER, $s.retval);} 
	;
/*
funcReturningInteger
	: keyLENGTH LPAREN stringExpr RPAREN
	| keyABS LPAREN numericExpr RPAREN
	| keyMOD LPAREN numericExpr COMMA numericExpr RPAREN
	;
*/

/*	
funcReturningBoolean
	: 
	;
*/
		
intItemList returns [List<IntegerValue> retval]
@init {retval = Lists.newArrayList();}
	: i1=intItem {retval.add($i1.retval);} (COMMA i2=intItem {retval.add($i2.retval);})*;
	
strItemList returns [List<StringValue> retval]
@init {retval = Lists.newArrayList();}
	: i1=strItem {retval.add($i1.retval);} (COMMA i2=strItem {retval.add($i2.retval);})*;
	
dateItemList returns [List<DateValue> retval]
@init {retval = Lists.newArrayList();}
	: d1=dateItem {retval.add($d1.retval);} (COMMA d2=dateItem {retval.add($d2.retval);})*;
	
intItem returns [IntegerValue retval]
	: n=numericExpr					{$intItem.retval = $n.retval;};

strItem returns [StringValue retval]
	: s=stringExpr					{$strItem.retval = $s.retval;};

dateItem returns [DateValue retval]
	: d=dateExpr					{$dateItem.retval = $d.retval;};

qstringList returns [List<String> retval]
@init {retval = Lists.newArrayList();}
	: qstring[retval] (COMMA qstring[retval])*;

column [List<String> list]	
	: charstr=varRef 				{if (list != null) list.add($charstr.text);};

schemaDesc returns [List<VarDesc> retval]
@init {retval = Lists.newArrayList();}
	: (varDesc[retval] (COMMA varDesc[retval])*)?;
	
varDesc [List<VarDesc> list] 
	: v=varRefList keyAS t=varType			{list.addAll(VarDesc.getList($v.retval, $t.text));}
	;

varRefList returns [List<String> retval]
@init {retval = Lists.newArrayList();}
	: v1=varRef {retval.add($v1.text);} (COMMA v2=varRef {retval.add($v2.text);})*
	;
	
varType	: ID;
		
varRef	
	: ID ((DOT | COLON) ID)*			
	;

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
@init {final StringBuilder sbuf = new StringBuilder();}	
	: DQUOTE (options {greedy=false;} : any=. {sbuf.append((char)$any);})* DQUOTE {setText(sbuf.toString());}
	| SQUOTE (options {greedy=false;} : any=. {sbuf.append((char)$any);})* SQUOTE {setText(sbuf.toString());}
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
keyFILTER	: {isKeyword(input, "FILTER")}? ID;
keyWITH		: {isKeyword(input, "WITH")}? ID;
keyFROM 	: {isKeyword(input, "FROM")}? ID;
keySET 		: {isKeyword(input, "SET")}? ID;
keyIN 		: {isKeyword(input, "IN")}? ID;
keyIS 		: {isKeyword(input, "IS")}? ID;
keyIF 		: {isKeyword(input, "IF")}? ID;
keyTHEN 	: {isKeyword(input, "THEN")}? ID;
keyELSE 	: {isKeyword(input, "ELSE")}? ID;
//keyEND	 : {isKeyword(input, "END")}? ID;
keyAS 		: {isKeyword(input, "AS")}? ID;
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
keyIGNORE_CASE 	: {isKeyword(input, "IGNORE_CASE")}? ID;
keyNOW	 	: {isKeyword(input, "NOW")}? ID;
keyYESTERDAY	: {isKeyword(input, "YESTERDAY")}? ID;
keyTOMORROW	: {isKeyword(input, "TOMORROW")}? ID;
keyDATE		: {isKeyword(input, "DATE")}? ID;
keyCLIENT	: {isKeyword(input, "CLIENT")}? ID;
keySERVER	: {isKeyword(input, "SERVER")}? ID;
keyVERSIONS	: {isKeyword(input, "VERSIONS")}? ID;
keyTIME		: {isKeyword(input, "TIME")}? ID;
keyRANGE	: {isKeyword(input, "RANGE")}? ID;
keyKEYS		: {isKeyword(input, "KEYS")}? ID;
