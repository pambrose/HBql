grammar HBql;

options {superClass=HBaseParser;}

tokens {
	DOT = '.';
	DOLLAR = '$';
	COLON = ':';
	QMARK = '?';
	STAR = '*';
	DIV = '/';
	COMMA = ',';
	PLUS = '+';
	MINUS = '-';
	MOD = '%';
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
	LCURLY = '{';
	RCURLY = '}';
}

@rulecatch {catch (RecognitionException re) {handleRecognitionException(re);}}

@header {
package org.apache.hadoop.hbase.hbql.query.antlr;
import org.apache.hadoop.hbase.hbql.query.expr.*;
import org.apache.hadoop.hbase.hbql.query.expr.node.*;
import org.apache.hadoop.hbase.hbql.query.expr.predicate.*;
import org.apache.hadoop.hbase.hbql.query.expr.value.func.*;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.*;
import org.apache.hadoop.hbase.hbql.query.expr.value.var.*;
import org.apache.hadoop.hbase.hbql.query.antlr.args.*;
import org.apache.hadoop.hbase.hbql.query.antlr.cmds.*;
import org.apache.hadoop.hbase.hbql.query.antlr.*;
import org.apache.hadoop.hbase.hbql.query.util.*;
import org.apache.hadoop.hbase.hbql.query.schema.*;
import java.util.Date;
}

@lexer::header {
package org.apache.hadoop.hbase.hbql.query.antlr;
import org.apache.hadoop.hbase.hbql.query.util.*;
}

selectStmt [Schema es] returns [QueryArgs retval]
	: keySELECT (STAR | c=columnList) keyFROM t=ID 	{setSchema($t.text);}
	  w=whereValue[es]?				{retval = new QueryArgs($c.retval, $t.text, $w.retval, getSchema());};

columnList returns [List<String> retval]
@init {retval = Lists.newArrayList();}
	: c1=column {retval.add($c1.text);} (COMMA c2=column {retval.add($c2.text);})*;

connectionExec returns [ConnectionCmd retval]
options {backtrack=true;}	
	: keyDROP keyTABLE t=ID 		 	{retval = new DropCmd($t.text);}
	| keyDISABLE keyTABLE t=ID 		 	{retval = new DisableCmd($t.text);}
	| keyENABLE keyTABLE t=ID 		 	{retval = new EnableCmd($t.text);}
	| keyDESCRIBE keyTABLE t=ID 			{retval = new DescribeCmd($t.text);}
	| keySHOW keyTABLES 		 		{retval = new ShowCmd();}
	| keySET i=ID EQ? v=QUOTED	 		{retval = new SetCmd($i.text, $v.text);}
	| cr=createStmt					{retval = $cr.retval;}
	| del=deleteStmt		 		{retval = $del.retval;}
	;

schemaExec returns [SchemaManagerCmd retval]
	: def=defineStmt 				{retval = $def.retval;}
	;

createStmt returns [CreateCmd retval]
	: keyCREATE keyTABLE keyUSING t=ID 		{retval = new CreateCmd($t.text);}
	;
	
defineStmt returns [DefineCmd retval]
	: keyDEFINE keyTABLE t=ID (keyALIAS alias=ID)? LPAREN a=attribList RPAREN
							{retval = new DefineCmd($t.text, $alias.text, $a.retval);};

attribList returns [List<VarDesc> retval] 
@init {retval = Lists.newArrayList();}
	: (a1=defineAttrib {retval.add($a1.retval);} (COMMA a2=defineAttrib {retval.add($a2.retval);})*)?;
	
defineAttrib returns [VarDesc retval]
	: c=ID t=ID 					{retval = VarDesc.newVarDesc($c.text, $c.text, $t.text);}
	| c=ID t=ID keyALIAS a=ID			{retval = VarDesc.newVarDesc($a.text, $c.text, $t.text);};

deleteStmt  returns [DeleteCmd retval]
@init {Schema schema = null;}
	: keyDELETE keyFROM t=ID 			{schema = setSchema($t.text);}
	  w=whereValue[schema]?				{retval = new DeleteCmd($t.text, $w.retval);};

whereValue [Schema es] returns [WhereArgs retval]
@init {retval = new WhereArgs();}
	: keyWITH
	  k=keysRange?					{retval.setKeyRangeArgs($k.retval);}
	  t=time?					{retval.setDateRangeArgs($t.retval);}	
	  v=versions?					{retval.setVersionArgs($v.retval);}
	  l=scanLimit?					{retval.setScanLimitArgs($l.retval);}
	  q=queryLimit?					{retval.setQueryLimitArgs($q.retval);}
	  s=serverFilter[es]?				{retval.setServerExprTree($s.retval);}
	  c=clientFilter[es]?				{retval.setClientExprTree($c.retval);}
	;

keysRange returns [KeyRangeArgs retval]
	: keyKEYS k=keyRangeList			{retval = new KeyRangeArgs($k.retval);}	
	| keyKEYS keyALL				{retval = new KeyRangeArgs();}	
	;
	
time returns [DateRangeArgs retval]
	: keyTIME keyRANGE d1=rangeExpr keyTO d2=rangeExpr		
							{retval = new DateRangeArgs($d1.retval, $d2.retval);}
	| keyTIME keySTAMP d1=rangeExpr			{retval = new DateRangeArgs($d1.retval, $d1.retval);}
	;
		
// Range Values
rangeExpr returns [DateValue retval]
@init {List<DateValue> exprList = Lists.newArrayList(); List<GenericCalcExpr.OP> opList = Lists.newArrayList();}
	: m=rangePrimary {exprList.add($m.retval);} (op=plusMinus n=rangePrimary {opList.add($op.retval); exprList.add($n.retval);})*
							{$rangeExpr.retval = getLeftAssociativeDateValues(exprList, opList);}
	;

rangePrimary returns [DateValue retval]
	: d1=rangeVal					{retval = $d1.retval;}
	| LPAREN d2=rangePrimary RPAREN			{retval = $d2.retval;}
	;

rangeVal returns [DateValue retval]
	: d2=funcReturningDatetime			{retval = $d2.retval;}
	| keyIF e=orExpr keyTHEN r1=rangeExpr keyELSE r2=rangeExpr keyEND	
							{retval = new DateTernary($e.retval, $r1.retval, $r2.retval);}
	;

versions returns [VersionArgs retval]
	: keyVERSIONS v=integerLiteral			{retval = new VersionArgs($v.retval);}
	| keyVERSIONS keyMAX				{retval = new VersionArgs(new NumberLiteral(-999));}
	;
	
scanLimit returns [LimitArgs retval]
	: keySCAN keyLIMIT v=integerLiteral		{retval = new LimitArgs($v.retval);};
	
queryLimit returns [LimitArgs retval]
	: keyQUERY keyLIMIT v=integerLiteral		{retval = new LimitArgs($v.retval);};
	
clientFilter [Schema es] returns [ExprTree retval]
	: keyCLIENT keyFILTER keyWHERE w=descWhereExpr[es]	
							{retval = $w.retval;};
	
serverFilter [Schema es] returns [ExprTree retval]
	: keySERVER keyFILTER keyWHERE w=descWhereExpr[es]	
							{retval = $w.retval;};
	
keyRangeList returns [List<KeyRangeArgs.Range> retval]
@init {retval = Lists.newArrayList();}
	:  k1=keyRange {retval.add($k1.retval);} (COMMA k2=keyRange {retval.add($k2.retval);})*
	;
	
keyRange returns [KeyRangeArgs.Range retval]
	: q1=QUOTED keyTO keyLAST			{retval = new KeyRangeArgs.Range($q1.text, KeyRangeArgs.Type.LAST);}
	//| q1=QUOTED 					{retval = new KeyRangeArgs.Range($q1.text, $q1.text);}
	| q1=QUOTED keyTO q2=QUOTED			{retval = new KeyRangeArgs.Range($q1.text, $q2.text);}
	;
	
nodescWhereExpr [Schema es] returns [ExprTree retval]
@init {setSchema(es);}
	 : e=orExpr					{retval = ExprTree.newExprTree($e.retval);};

descWhereExpr [Schema es] returns [ExprTree retval]
@init {setSchema(es);}
	: s=schemaDesc? 				{if ($s.retval != null) setSchema($s.retval);}			
	  e=orExpr					{retval = ExprTree.newExprTree($e.retval); if ($s.retval != null) retval.setSchema($s.retval);};

value returns [ValueExpr retval]
	: o=orExpr					{retval = $o.retval;}
	/*
	| keyIF v1=value keyTHEN v2=value keyELSE v3=value keyEND	
							{retval = new ValueTernary($v1.retval, $v2.retval, $v3.retval);}
	| keyIF e=orExpr keyTHEN b1=orExpr keyELSE b2=orExpr keyEND	
							{retval = new BooleanTernary($e.retval, $b1.retval, $b2.retval);}
	*/
	;
				
orExpr returns [BooleanValue retval]
	: e1=andExpr (keyOR e2=orExpr)?			{$orExpr.retval = ($e2.text == null) ? $e1.retval : new CompareExpr($e1.retval, CompareExpr.OP.OR, $e2.retval);};

andExpr returns [BooleanValue retval]
	: e1=condFactor (keyAND e2=andExpr)?		{$andExpr.retval = ($e2.text == null) ? $e1.retval : new CompareExpr($e1.retval, CompareExpr.OP.AND, $e2.retval);};

condFactor returns [BooleanValue retval]			 
	: n=keyNOT? p=simpleCond			{retval = ($n.text != null) ? new CondFactor(true, $p.retval) :  $p.retval;};
	
simpleCond returns [BooleanValue retval]
options {backtrack=true;}	
	: b=booleanCond					{retval = $b.retval;}
	| c=compareExpr 				{retval = $c.retval;}
	;

compareExpr returns [BooleanValue retval]
	: v1=valueExpr o=compareOp v2=valueExpr 	{retval = new ValueCompare($v1.retval, $o.retval, $v2.retval);}	
	;

valueExpr returns [ValueExpr retval]
options {backtrack=true;}	
	: d=dateValue					{retval = $d.retval;}
	| s=stringValue					{retval = $s.retval;}
	| n=numberValue					{retval = $n.retval;}
	| b=booleanValue				{retval = $b.retval;}
	;

booleanCond returns [BooleanValue retval]
options {backtrack=true;}	
	: b=booleanParen				{retval = $b.retval;}
	| f=booleanFuncs				{retval = $f.retval;}
	;

booleanFuncs returns [BooleanValue retval]
options {backtrack=true;}	
	: s1=stringValue keyCONTAINS s2=stringValue	{retval = new BooleanFunction(GenericFunction.Type.CONTAINS, $s1.retval, $s2.retval);}
	| l=likeExpr					{retval = $l.retval;}
	| b1=betweenExpr				{retval = $b1.retval;}
	| i=inExpr					{retval = $i.retval;}
	| n=nullCompareExpr				{retval = $n.retval;}
	;

booleanParen returns [BooleanValue retval]
options {backtrack=true;}	
	: s=booleanValue  				{retval = $s.retval;}
	| LPAREN o=orExpr RPAREN			{retval = $o.retval;}
	;
	
// Boolean Value
booleanValue returns [BooleanValue retval]
options {backtrack=true;}	
	: b=booleanLiteral				{retval = $b.retval;}
	;
		
// Numeric calculations
numberValue returns [NumberValue retval] 
@init {List<NumberValue> exprList = Lists.newArrayList(); List<GenericCalcExpr.OP> opList = Lists.newArrayList(); }
	: m=multNumber {exprList.add($m.retval);} (op=plusMinus n=multNumber {opList.add($op.retval); exprList.add($n.retval);})*	
							{retval = getLeftAssociativeNumberValues(exprList, opList);};
	
multNumber returns [NumberValue retval]
@init {List<NumberValue> exprList = Lists.newArrayList(); List<GenericCalcExpr.OP> opList = Lists.newArrayList(); }
	: m=signedNumberPrimary {exprList.add($m.retval);} (op=multDiv n=signedNumberPrimary {opList.add($op.retval); exprList.add($n.retval);})*	
							{retval = getLeftAssociativeNumberValues(exprList, opList);};
	
signedNumberPrimary returns [NumberValue retval]
	: (s=plusMinus)? n=numberParen 			{$signedNumberPrimary.retval = ($s.retval == GenericCalcExpr.OP.MINUS) ? new NumberCalcExpr($n.retval, GenericCalcExpr.OP.NEGATIVE, null) :  $n.retval;};

numberParen returns [NumberValue retval]
	: n=numberCond					{retval = $n.retval;}
	| LPAREN s=numberValue RPAREN			{retval = $s.retval;}
	;
	   						 
numberCond returns [NumberValue retval]
	: l=numberVal					{retval = $l.retval;} 
	| keyIF e=orExpr keyTHEN n1=numberValue keyELSE n2=numberValue keyEND 	
							{retval = new NumberTernary($e.retval, $n1.retval, $n2.retval);}
	;

numberVal returns [NumberValue retval]
	: l=integerLiteral				{retval = $l.retval;} 
	| f=funcReturningInteger			{retval = $f.retval;}
	| i=numberAttribVar				{retval = $i.retval;}
	;
	
// String Values 
stringValue returns [StringValue retval]
	: s1=stringParen (p=PLUS s2=stringValue)?
							{retval = ($p.text == null) ? $s1.retval : new StringConcat($s1.retval, $s2.retval);}
	;

stringParen returns [StringValue retval]
	: s1=stringCond					{retval = $s1.retval;}
	| LPAREN s2=stringValue	RPAREN			{retval = $s2.retval;}						
	;

stringCond returns [StringValue retval]
	: s=stringPrimary				{retval = $s.retval;}
	| keyIF e=orExpr keyTHEN s1=stringValue keyELSE s2=stringValue keyEND	
							{retval = new StringTernary($e.retval, $s1.retval, $s2.retval);}
	;
				
stringPrimary returns [StringValue retval]
	: sl=stringLiteral				{retval = $sl.retval;}
	| f=funcReturningString				{retval = $f.retval;}
	| keyNULL					{retval = new StringNullLiteral();}
	| a=stringAttribVar				{retval = $a.retval;}
	;

// Date Values
dateValue returns [DateValue retval]
@init {List<DateValue> exprList = Lists.newArrayList(); 
       List<GenericCalcExpr.OP> opList = Lists.newArrayList();}
	: m=dateParen {exprList.add($m.retval);} (op=plusMinus n=dateParen {opList.add($op.retval); exprList.add($n.retval);})*	
							{retval = getLeftAssociativeDateValues(exprList, opList);};

dateParen returns [DateValue retval]
	: d1=dateCond					{retval = $d1.retval;}
	| LPAREN d2=dateParen RPAREN			{retval = $d2.retval;}
	;

dateCond returns [DateValue retval]
	: d=dateVal					{retval = $d.retval;}
	| keyIF e=orExpr keyTHEN r1=dateValue keyELSE r2=dateValue keyEND	
							{retval = new DateTernary($e.retval, $r1.retval, $r2.retval);}
	;
			
dateVal returns [DateValue retval]
	: d2=funcReturningDatetime			{retval = $d2.retval;}
	| d3=dateAttribVar				{retval = $d3.retval;} 			
	;

// Variables
numberAttribVar returns [NumberValue retval]
	: {isAttribType(input, FieldType.IntegerType)}? v=variableRef 
							{retval = (NumberValue)this.getValueExpr($v.text);};

stringAttribVar returns [StringValue retval]
	: {isAttribType(input, FieldType.StringType)}? v=variableRef 
							{retval = (StringValue)this.getValueExpr($v.text);};

dateAttribVar returns [DateValue retval]
	: {isAttribType(input, FieldType.DateType)}? v=variableRef 
							{retval = (DateValue)this.getValueExpr($v.text);};

// Literals		
stringLiteral returns [StringValue retval]
	: v=QUOTED 					{retval = new StringLiteral($v.text);};
	
integerLiteral returns [NumberValue retval]
	: v=INT						{retval = new IntegerLiteral(Integer.valueOf($v.text));};	

booleanLiteral returns [BooleanValue retval]
	: t=keyTRUE					{retval = new BooleanLiteral($t.text);}
	| f=keyFALSE					{retval = new BooleanLiteral($f.text);}
	;

// Functions
funcReturningDatetime returns [DateValue retval]
	: keyNOW LPAREN	RPAREN				{retval = new DateLiteral(DateLiteral.Type.NOW);}
	| keyMINDATE LPAREN RPAREN			{retval = new DateLiteral(DateLiteral.Type.MINDATE);}
	| keyMAXDATE LPAREN RPAREN			{retval = new DateLiteral(DateLiteral.Type.MAXDATE);}
	| keyDATE LPAREN s1=stringValue COMMA s2=stringValue RPAREN
							{retval = new DateExpr($s1.retval, $s2.retval);}
	| keyYEAR LPAREN n=numberValue RPAREN		{retval = new IntervalExpr(IntervalExpr.IntervalType.YEAR, $n.retval);}
	| keyWEEK LPAREN n=numberValue RPAREN		{retval = new IntervalExpr(IntervalExpr.IntervalType.WEEK, $n.retval);}
	| keyDAY LPAREN n=numberValue RPAREN		{retval = new IntervalExpr(IntervalExpr.IntervalType.DAY, $n.retval);}
	| keyHOUR LPAREN n=numberValue RPAREN		{retval = new IntervalExpr(IntervalExpr.IntervalType.HOUR, $n.retval);}
	| keyMINUTE LPAREN n=numberValue RPAREN		{retval = new IntervalExpr(IntervalExpr.IntervalType.MINUTE, $n.retval);}
	| keySECOND LPAREN n=numberValue RPAREN		{retval = new IntervalExpr(IntervalExpr.IntervalType.SECOND, $n.retval);}
	| keyMILLI LPAREN n=numberValue RPAREN		{retval = new IntervalExpr(IntervalExpr.IntervalType.MILLI, $n.retval);}
	;

funcReturningString returns [StringValue retval]
	: keyCONCAT LPAREN s1=stringValue COMMA s2=stringValue RPAREN
							{retval = new StringFunction(GenericFunction.Type.CONCAT, $s1.retval, $s2.retval);}
	| keySUBSTRING LPAREN s=stringValue COMMA n1=numberValue COMMA n2=numberValue RPAREN
							{retval = new Substring($s.retval, $n1.retval, $n2.retval);}
	| keyTRIM LPAREN s=stringValue RPAREN		{retval = new StringFunction(GenericFunction.Type.TRIM, $s.retval);}
	| keyLOWER LPAREN s=stringValue RPAREN		{retval = new StringFunction(GenericFunction.Type.LOWER, $s.retval);} 
	| keyUPPER LPAREN s=stringValue RPAREN		{retval = new StringFunction(GenericFunction.Type.UPPER, $s.retval);} 
	| keyREPLACE LPAREN s1=stringValue COMMA s2=stringValue COMMA s3=stringValue RPAREN		
							{retval = new StringFunction(GenericFunction.Type.REPLACE, $s1.retval, $s2.retval, $s3.retval);} 
	;

funcReturningInteger returns [NumberValue retval]
	: keyLENGTH LPAREN s=stringValue RPAREN		{retval = new NumberFunction(GenericFunction.Type.LENGTH, $s.retval);}
	| keyINDEXOF LPAREN s1=stringValue COMMA s2=stringValue RPAREN
							{retval = new NumberFunction(GenericFunction.Type.INDEXOF, $s1.retval, $s2.retval);}
	//| keyABS LPAREN numericExpr RPAREN
	;


betweenExpr returns [BooleanValue retval]
options {backtrack=true;}	
	: d1=dateValue n=keyNOT? keyBETWEEN d2=dateValue keyAND d3=dateValue
							{retval = new DateBetweenStmt($d1.retval, ($n.text != null), $d2.retval, $d3.retval);}
	| n1=numberValue n=keyNOT? keyBETWEEN n2=numberValue keyAND n3=numberValue		
							{retval = new NumberBetweenStmt($n1.retval, ($n.text != null), $n2.retval, $n3.retval);}
	| s1=stringValue n=keyNOT? keyBETWEEN s2=stringValue keyAND s3=stringValue		
							{retval = new StringBetweenStmt($s1.retval, ($n.text != null), $s2.retval, $s3.retval);}
	;

likeExpr returns [BooleanValue retval]
	: s1=stringValue n=keyNOT? keyLIKE s2=stringValue 
							{retval = new LikeStmt($s1.retval, ($n.text != null), $s2.retval);};

inExpr returns [BooleanValue retval]
options {backtrack=true;}	
	: a3=dateValue n=keyNOT? keyIN LPAREN d=dateItemList RPAREN			
							{retval = new DateInStmt($a3.retval, ($n.text != null), $d.retval);} 
	| a1=numberValue n=keyNOT? keyIN LPAREN i=numberItemList RPAREN			
							{retval = new NumberInStmt($a1.retval,($n.text != null), $i.retval);} 
	| a2=stringValue n=keyNOT? keyIN LPAREN s=stringItemList RPAREN			
							{retval = new StringInStmt($a2.retval, ($n.text != null), $s.retval);} 
	;
	
nullCompareExpr returns [BooleanValue retval]
	: s=stringValue keyIS (n=keyNOT)? keyNULL	{retval = new StringNullCompare(($n.text != null), $s.retval);}	
	;	
		
numberItemList returns [List<NumberValue> retval]
@init {retval = Lists.newArrayList();}
	: i1=numberItem {retval.add($i1.retval);} (COMMA i2=numberItem {retval.add($i2.retval);})*;
	
stringItemList returns [List<StringValue> retval]
@init {retval = Lists.newArrayList();}
	: i1=stringItem {retval.add($i1.retval);} (COMMA i2=stringItem {retval.add($i2.retval);})*;
	
dateItemList returns [List<DateValue> retval]
@init {retval = Lists.newArrayList();}
	: d1=dateItem {retval.add($d1.retval);} (COMMA d2=dateItem {retval.add($d2.retval);})*;
	
numberItem returns [NumberValue retval]
	: n=numberValue					{retval = $n.retval;};

stringItem returns [StringValue retval]
	: s=stringValue					{retval = $s.retval;};

dateItem returns [DateValue retval]
	: d=dateValue					{retval = $d.retval;};

qstringList returns [List<String> retval]
@init {retval = Lists.newArrayList();}
	: qstring[retval] (COMMA qstring[retval])*;

column 	: c=variableRef;
	
schemaDesc returns [Schema retval]
	: LCURLY a=attribList RCURLY			{retval = HUtil.newDefinedSchema(input, $a.retval);};
	
compareOp returns [GenericCompare.OP retval]
	: EQ 						{retval = GenericCompare.OP.EQ;}
	| GT 						{retval = GenericCompare.OP.GT;}
	| GTEQ 						{retval = GenericCompare.OP.GTEQ;}
	| LT 						{retval = GenericCompare.OP.LT;}
	| LTEQ 						{retval = GenericCompare.OP.LTEQ;}
	| (LTGT | BANGEQ)				{retval = GenericCompare.OP.NOTEQ;}
	;
			
variableRef
	: ID;

paramRef
	: PARAM ;
	
qstring	[List<String> list]
	: QUOTED 					{if (list != null) list.add($QUOTED.text);};

plusMinus returns [GenericCalcExpr.OP retval]
	: PLUS						{retval = GenericCalcExpr.OP.PLUS;}
	| MINUS						{retval = GenericCalcExpr.OP.MINUS;}
	;
	
multDiv returns [GenericCalcExpr.OP retval]
	: STAR						{retval = GenericCalcExpr.OP.MULT;}
	| DIV						{retval = GenericCalcExpr.OP.DIV;}
	| MOD						{retval = GenericCalcExpr.OP.MOD;}
	;
		
INT	: DIGIT+;
ID	: CHAR (CHAR | DOT | MINUS | DOLLAR | DIGIT)* 		// DOOLAR is for inner class table names
	| CHAR (CHAR | DOT | MINUS | DIGIT)* COLON (CHAR | DOT | MINUS | DIGIT)*
	;
PARAM	: COLON CHAR (CHAR | DOT | MINUS | DIGIT)*;	
	
//PARAM	: COLON (CHAR | DIGIT  | DOT)*;
 
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
keyDEFINE 	: {isKeyword(input, "DEFINE")}? ID;
keyUSING 	: {isKeyword(input, "USING")}? ID;
keyDESCRIBE 	: {isKeyword(input, "DESCRIBE")}? ID;
keySHOW 	: {isKeyword(input, "SHOW")}? ID;
keyENABLE 	: {isKeyword(input, "ENABLE")}? ID;
keyDISABLE 	: {isKeyword(input, "DISABLE")}? ID;
keyDROP 	: {isKeyword(input, "DROP")}? ID;
keyTABLE 	: {isKeyword(input, "TABLE")}? ID;
keyTABLES 	: {isKeyword(input, "TABLES")}? ID;
keyWHERE	: {isKeyword(input, "WHERE")}? ID;
keyWITH		: {isKeyword(input, "WITH")}? ID;
keyFILTER	: {isKeyword(input, "FILTER")}? ID;
keyFROM 	: {isKeyword(input, "FROM")}? ID;
keySET 		: {isKeyword(input, "SET")}? ID;
keyIN 		: {isKeyword(input, "IN")}? ID;
keyIS 		: {isKeyword(input, "IS")}? ID;
keyIF 		: {isKeyword(input, "IF")}? ID;
keyALIAS	: {isKeyword(input, "ALIAS")}? ID;
keyTHEN 	: {isKeyword(input, "THEN")}? ID;
keyELSE 	: {isKeyword(input, "ELSE")}? ID;
keyEND 		: {isKeyword(input, "END")}? ID;
keyLAST		: {isKeyword(input, "LAST")}? ID;
keySCAN 	: {isKeyword(input, "SCAN")}? ID;
keyQUERY 	: {isKeyword(input, "QUERY")}? ID;
keyLIMIT 	: {isKeyword(input, "LIMIT")}? ID;
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
keyMINDATE	: {isKeyword(input, "MINDATE")}? ID;
keyMAXDATE	: {isKeyword(input, "MAXDATE")}? ID;
keyDATE		: {isKeyword(input, "DATE")}? ID;
keyYEAR		: {isKeyword(input, "YEAR")}? ID;
keyWEEK		: {isKeyword(input, "WEEK")}? ID;
keyDAY		: {isKeyword(input, "DAY")}? ID;
keyHOUR		: {isKeyword(input, "HOUR")}? ID;
keyMINUTE	: {isKeyword(input, "MINUTE")}? ID;
keySECOND	: {isKeyword(input, "SECOND")}? ID;
keyMILLI	: {isKeyword(input, "MILLI")}? ID;
keyCLIENT	: {isKeyword(input, "CLIENT")}? ID;
keySERVER	: {isKeyword(input, "SERVER")}? ID;
keyVERSIONS	: {isKeyword(input, "VERSIONS")}? ID;
keyTIME		: {isKeyword(input, "TIME")}? ID;
keyRANGE	: {isKeyword(input, "RANGE")}? ID;
keySTAMP	: {isKeyword(input, "STAMP")}? ID;
keyMAX		: {isKeyword(input, "MAX")}? ID;
keyKEYS		: {isKeyword(input, "KEYS")}? ID;
keyALL		: {isKeyword(input, "ALL")}? ID;
keyLENGTH	: {isKeyword(input, "LENGTH")}? ID;
keyCONTAINS	: {isKeyword(input, "CONTAINS")}? ID;
keyINDEXOF	: {isKeyword(input, "INDEXOF")}? ID;
keyREPLACE	: {isKeyword(input, "REPLACE")}? ID;
