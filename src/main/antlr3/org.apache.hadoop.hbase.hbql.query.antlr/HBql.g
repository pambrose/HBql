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
	: keyTIME keyRANGE d1=valueExpr keyTO d2=valueExpr		
							{retval = new DateRangeArgs($d1.retval, $d2.retval);}
	| keyTIME keySTAMP d1=valueExpr			{retval = new DateRangeArgs($d1.retval, $d1.retval);}
	;
		
versions returns [VersionArgs retval]
	: keyVERSIONS v=valueExpr			{retval = new VersionArgs($v.retval);}
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
	:  k1=keyRange {retval.add($k1.retval);} (COMMA k2=keyRange {retval.add($k2.retval);})*;
	
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
	  e=orExpr					{retval = ExprTree.newExprTree($e.retval); if ($s.retval != null) retval.setSchema($s.retval);}
	;
				
orExpr returns [BooleanValue retval]
	: e1=andExpr (keyOR e2=orExpr)?			{$orExpr.retval = ($e2.text == null) ? $e1.retval : new CompareExpr($e1.retval, Operator.OR, $e2.retval);};

andExpr returns [BooleanValue retval]
	: e1=condFactor (keyAND e2=andExpr)?		{$andExpr.retval = ($e2.text == null) ? $e1.retval : new CompareExpr($e1.retval, Operator.AND, $e2.retval);};

condFactor returns [BooleanValue retval]			 
	: n=keyNOT? p=booleanPrimary			{retval = ($n.text != null) ? new CondFactor(true, $p.retval) :  $p.retval;};

booleanPrimary returns [BooleanValue retval]
options {backtrack=true;}	
	: b=eqneCompare					{retval = $b.retval;}
	| f=booleanFuncs				{retval = $f.retval;}
	;

booleanFuncs returns [BooleanValue retval]
options {backtrack=true;}	
	: s1=valueExpr keyCONTAINS s2=valueExpr		{retval = new BooleanFunction(FunctionType.CONTAINS, $s1.retval, $s2.retval);}
	| s1=valueExpr n=keyNOT? keyLIKE s2=valueExpr 
							{retval = new LikeStmt($s1.retval, ($n.text != null), $s2.retval);}
	| s1=valueExpr n=keyNOT? keyBETWEEN s2=valueExpr keyAND s3=valueExpr		
							{retval = new ValueBetweenStmt($s1.retval, ($n.text != null), $s2.retval, $s3.retval);}
	| s1=valueExpr n=keyNOT? keyIN LPAREN s=valueItemList RPAREN			
							{retval = new ValueInStmt($s1.retval, ($n.text != null), $s.retval);} 
	| s1=valueExpr keyIS (n=keyNOT)? keyNULL	{retval = new ValueNullCompare(($n.text != null), $s1.retval);}	
	;
	
eqneCompare returns [BooleanValue retval]
options {backtrack=true;}	
	: b=ltgtCompare					{retval = $b.retval;}
	| v1=valueExpr o=eqneOp v2=valueExpr 		{retval = new ValueCompare($v1.retval, $o.retval, $v2.retval);}	
	;

ltgtCompare returns [BooleanValue retval]
options {backtrack=true;}	
	: b=booleanParen				{retval = $b.retval;}
	| v1=valueExpr o=ltgtOp v2=valueExpr 		{retval = new ValueCompare($v1.retval, $o.retval, $v2.retval);}	
	;

booleanParen returns [BooleanValue retval]
options {backtrack=true;}	
	: s=valueExpr  					{retval = new BooleanExpr($s.retval);}
	| LPAREN o=orExpr RPAREN			{retval = $o.retval;}
	;
		
// Literals		
stringLiteral returns [StringValue retval]
	: v=QUOTED 					{retval = new StringLiteral($v.text);};
	
integerLiteral returns [NumberValue retval]
	: v=INT						{retval = new IntegerLiteral(Integer.valueOf($v.text));};	

booleanLiteral returns [BooleanValue retval]
	: t=keyTRUE					{retval = new BooleanLiteral($t.text);}
	| f=keyFALSE					{retval = new BooleanLiteral($f.text);}
	;

valueExpr returns [ValueExpr retval]
	: a=addExpr					{retval = $a.retval;}
	;
			
// Numeric calculations
addExpr returns [ValueExpr retval] 
@init {List<ValueExpr> exprList = Lists.newArrayList(); List<Operator> opList = Lists.newArrayList(); }
	: m=multExpr {exprList.add($m.retval);} (op=plusMinus n=multExpr {opList.add($op.retval); exprList.add($n.retval);})*	
							{retval = getLeftAssociativeValueExprs(exprList, opList);};
	
multExpr returns [ValueExpr retval]
@init {List<ValueExpr> exprList = Lists.newArrayList(); List<Operator> opList = Lists.newArrayList(); }
	: m=signedExpr {exprList.add($m.retval);} (op=multDiv n=signedExpr {opList.add($op.retval); exprList.add($n.retval);})*	
							{retval = getLeftAssociativeValueExprs(exprList, opList);};
	
signedExpr returns [ValueExpr retval]
	: (s=plusMinus)? n=parenExpr 			{$signedExpr.retval = ($s.retval == Operator.MINUS) ? new ValueCalcExpr($n.retval, Operator.NEGATIVE, null) :  $n.retval;};

parenExpr returns [ValueExpr retval]
options {backtrack=true;}	
	: n=atomExpr					{retval = $n.retval;}
	| LPAREN s=valueExpr RPAREN			{retval = $s.retval;}
	| LPAREN o=orExpr RPAREN			{retval = $o.retval;}
	;
	   						 
atomExpr returns [ValueExpr retval]
	: v=valueAtom					{retval = $v.retval;} 
	| f1=funcReturningInteger			{retval = $f1.retval;}
	| f2=funcReturningString			{retval = $f2.retval;}
	| d2=funcReturningDatetime			{retval = $d2.retval;}
	| keyIF v1=orExpr keyTHEN v2=valueExpr keyELSE v3=valueExpr keyEND	
							{retval = new ValueTernary($v1.retval, $v2.retval, $v3.retval);}
	;

// Value Atom
valueAtom returns [ValueExpr retval]
	: s=stringLiteral				{retval = $s.retval;}
	| i=integerLiteral				{retval = $i.retval;}
	| b=booleanLiteral				{retval = $b.retval;}
	| keyNULL					{retval = new StringNullLiteral();}
	| v=varRef					{retval = this.getVariableRef($v.text);}
	| p=paramRef
	;
						
// Functions
funcReturningDatetime returns [DateValue retval]
	: keyNOW LPAREN	RPAREN				{retval = new DateLiteral(DateLiteral.Type.NOW);}
	| keyMINDATE LPAREN RPAREN			{retval = new DateLiteral(DateLiteral.Type.MINDATE);}
	| keyMAXDATE LPAREN RPAREN			{retval = new DateLiteral(DateLiteral.Type.MAXDATE);}
	| keyDATE LPAREN s1=valueExpr COMMA s2=valueExpr RPAREN
							{retval = new DateExpr($s1.retval, $s2.retval);}
	| keyYEAR LPAREN n=valueExpr RPAREN		{retval = new IntervalExpr(IntervalExpr.IntervalType.YEAR, $n.retval);}
	| keyWEEK LPAREN n=valueExpr RPAREN		{retval = new IntervalExpr(IntervalExpr.IntervalType.WEEK, $n.retval);}
	| keyDAY LPAREN n=valueExpr RPAREN		{retval = new IntervalExpr(IntervalExpr.IntervalType.DAY, $n.retval);}
	| keyHOUR LPAREN n=valueExpr RPAREN		{retval = new IntervalExpr(IntervalExpr.IntervalType.HOUR, $n.retval);}
	| keyMINUTE LPAREN n=valueExpr RPAREN		{retval = new IntervalExpr(IntervalExpr.IntervalType.MINUTE, $n.retval);}
	| keySECOND LPAREN n=valueExpr RPAREN		{retval = new IntervalExpr(IntervalExpr.IntervalType.SECOND, $n.retval);}
	| keyMILLI LPAREN n=valueExpr RPAREN		{retval = new IntervalExpr(IntervalExpr.IntervalType.MILLI, $n.retval);}
	;

funcReturningString returns [StringValue retval]
	: keyCONCAT LPAREN s1=valueExpr COMMA s2=valueExpr RPAREN
							{retval = new StringFunction(FunctionType.CONCAT, $s1.retval, $s2.retval);}
	| keySUBSTRING LPAREN s=valueExpr COMMA n1=valueExpr COMMA n2=valueExpr RPAREN
							{retval = new StringFunction(FunctionType.SUBSTRING, $s.retval, $n1.retval, $n2.retval);}
	| keyTRIM LPAREN s=valueExpr RPAREN		{retval = new StringFunction(FunctionType.TRIM, $s.retval);}
	| keyLOWER LPAREN s=valueExpr RPAREN		{retval = new StringFunction(FunctionType.LOWER, $s.retval);} 
	| keyUPPER LPAREN s=valueExpr RPAREN		{retval = new StringFunction(FunctionType.UPPER, $s.retval);} 
	| keyREPLACE LPAREN s1=valueExpr COMMA s2=valueExpr COMMA s3=valueExpr RPAREN		
							{retval = new StringFunction(FunctionType.REPLACE, $s1.retval, $s2.retval, $s3.retval);} 
	;

funcReturningInteger returns [NumberValue retval]
	: keyLENGTH LPAREN s=valueExpr RPAREN		{retval = new NumberFunction(FunctionType.LENGTH, $s.retval);}
	| keyINDEXOF LPAREN s1=valueExpr COMMA s2=valueExpr RPAREN
							{retval = new NumberFunction(FunctionType.INDEXOF, $s1.retval, $s2.retval);}
	//| keyABS LPAREN numericExpr RPAREN
	;
			
valueItemList returns [List<ValueExpr> retval]
@init {retval = Lists.newArrayList();}
	: i1=valueExpr {retval.add($i1.retval);} (COMMA i2=valueExpr {retval.add($i2.retval);})*;
	

qstringList returns [List<String> retval]
@init {retval = Lists.newArrayList();}
	: qstring[retval] (COMMA qstring[retval])*;

column 	: c=varRef;
	
schemaDesc returns [Schema retval]
	: LCURLY a=attribList RCURLY			{retval = HUtil.newDefinedSchema(input, $a.retval);};
	
ltgtOp returns [Operator retval]
	: GT 						{retval = Operator.GT;}
	| GTEQ 						{retval = Operator.GTEQ;}
	| LT 						{retval = Operator.LT;}
	| LTEQ 						{retval = Operator.LTEQ;}
	;
			
eqneOp returns [Operator retval]
	: EQ 						{retval = Operator.EQ;}
	| (LTGT | BANGEQ)				{retval = Operator.NOTEQ;}
	;
				
qstring	[List<String> list]
	: QUOTED 					{if (list != null) list.add($QUOTED.text);};

plusMinus returns [Operator retval]
	: PLUS						{retval = Operator.PLUS;}
	| MINUS						{retval = Operator.MINUS;}
	;
	
multDiv returns [Operator retval]
	: STAR						{retval = Operator.MULT;}
	| DIV						{retval = Operator.DIV;}
	| MOD						{retval = Operator.MOD;}
	;

varRef 	: ID;

paramRef: PARAM;
		
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
