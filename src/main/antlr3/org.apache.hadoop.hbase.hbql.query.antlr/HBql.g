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
	| q1=QUOTED keyTO q2=QUOTED			{retval = new KeyRangeArgs.Range($q1.text, $q2.text);}
	;
	
nodescWhereExpr [Schema es] returns [ExprTree retval]
@init {setSchema(es);}
	 : e=booleanExpr				{retval = ExprTree.newExprTree($e.retval);};

descWhereExpr [Schema es] returns [ExprTree retval]
@init {setSchema(es);}
	: s=schemaDesc? 				{if ($s.retval != null) setSchema($s.retval);}			
	  e=booleanExpr					{retval = ExprTree.newExprTree($e.retval); if ($s.retval != null) retval.setSchema($s.retval);}
	;

// Boolean Expressions				
booleanExpr returns [BooleanValue retval]
	: e1=andExpr (keyOR e2=booleanExpr)?		{$booleanExpr.retval = ($e2.text == null) ? $e1.retval : new BooleanCompare($e1.retval, Operator.OR, $e2.retval);};

andExpr returns [BooleanValue retval]
	: e1=booleanNot (keyAND e2=andExpr)?		{$andExpr.retval = ($e2.text == null) ? $e1.retval : new BooleanCompare($e1.retval, Operator.AND, $e2.retval);};

booleanNot returns [BooleanValue retval]			 
	: (n=keyNOT)? p=booleanPrimary			{retval = ($n.text != null) ? new BooleanNot(true, $p.retval) :  $p.retval;};

booleanPrimary returns [BooleanValue retval]
	: b=eqneCompare					{retval = $b.retval;};

eqneCompare returns [BooleanValue retval]
options {backtrack=true;}	
	: v1=valueExpr o=eqneOp v2=valueExpr 		{retval = new DelegateCompare($v1.retval, $o.retval, $v2.retval);}	
	| c=ltgtCompare					{retval = $c.retval;}
	;

ltgtCompare returns [BooleanValue retval]
options {backtrack=true;}	
	: v1=valueExpr o=ltgtOp v2=valueExpr		{retval = new DelegateCompare($v1.retval, $o.retval, $v2.retval);}
	| p=booleanParen				{retval = $p.retval;}
	;

booleanParen returns [BooleanValue retval]
options {backtrack=true;}	
	: LPAREN o=booleanExpr RPAREN			{retval = $o.retval;}
	| b=booleanVal					{retval = $b.retval;}
	;

booleanVal returns [BooleanValue retval]
options {backtrack=true;}	
	: f=booleanFuncs				{retval = $f.retval;}
	| b=booleanAtom					{retval = new BooleanExpr($b.retval);}
	;
	
booleanAtom returns [GenericValue retval]
	: b=booleanLiteral				{retval = $b.retval;}
	| v=varRef					{retval = this.getVariable($v.text);}
	| p=paramRef					{retval = new NamedParameter($p.text);}
	;
									
booleanFuncs returns [BooleanValue retval]
options {backtrack=true; memoize=true;}	
	: s1=valueExpr n=keyNOT? keyCONTAINS s2=valueExpr		
							{retval = new ContainsStmt($s1.retval, ($n.text != null), $s2.retval);}
	| s1=valueExpr n=keyNOT? keyLIKE s2=valueExpr 	{retval = new LikeStmt($s1.retval, ($n.text != null), $s2.retval);}
	| s1=valueExpr n=keyNOT? keyBETWEEN s2=valueExpr keyAND s3=valueExpr		
							{retval = new DelegateBetweenStmt($s1.retval, ($n.text != null), $s2.retval, $s3.retval);}
	| s1=valueExpr n=keyNOT? keyIN LPAREN l=valueItemList RPAREN			
							{retval = new DelegateInStmt($s1.retval, ($n.text != null), $l.retval);} 
	| s1=valueExpr keyIS (n=keyNOT)? keyNULL	{retval = new DelegateNullCompare(($n.text != null), $s1.retval);}	
	;

valueItemList returns [List<GenericValue> retval]
@init {retval = Lists.newArrayList();}
	: i1=valueExpr {retval.add($i1.retval);} (COMMA i2=valueExpr {retval.add($i2.retval);})*;
	
// Value Expressions
valueExpr returns [GenericValue retval] 
options {backtrack=true; memoize=true;}	
	: v=valuePrimary				{retval = $v.retval;}
	| LPAREN o=booleanExpr RPAREN			{retval = $o.retval;}
	;
	
valuePrimary returns [GenericValue retval] 
@init {List<GenericValue> exprList = Lists.newArrayList(); List<Operator> opList = Lists.newArrayList(); }
	: m=multExpr {exprList.add($m.retval);} (op=plusMinus n=multExpr {opList.add($op.retval); exprList.add($n.retval);})*	
							{retval = getLeftAssociativeGenericValues(exprList, opList);}
	;
	
multExpr returns [GenericValue retval]
@init {List<GenericValue> exprList = Lists.newArrayList(); List<Operator> opList = Lists.newArrayList(); }
	: m=signedExpr {exprList.add($m.retval);} (op=multDiv n=signedExpr {opList.add($op.retval); exprList.add($n.retval);})*	
							{retval = getLeftAssociativeGenericValues(exprList, opList);};
	
signedExpr returns [GenericValue retval]
	: (s=plusMinus)? n=parenExpr 			{$signedExpr.retval = ($s.retval == Operator.MINUS) ? new DelegateCalculation($n.retval, Operator.NEGATIVE, new IntegerLiteral(0)) :  $n.retval;};

parenExpr returns [GenericValue retval]
options {backtrack=true; memoize=true;}	
	: n=atomExpr					{retval = $n.retval;}
	| LPAREN s=valueExpr RPAREN			{retval = $s.retval;}
	;
	   						 
atomExpr returns [GenericValue retval]
	: v=valueAtom					{retval = $v.retval;} 
	| f=valueFunctions				{retval = $f.retval;}
	;

// Value Atom
valueAtom returns [GenericValue retval]
	: s=stringLiteral				{retval = $s.retval;}
	| i=integerLiteral				{retval = $i.retval;}
	| b=booleanAtom					{retval = $b.retval;}
	| keyNULL					{retval = new StringNullLiteral();}
	;

// Literals		
stringLiteral returns [GenericValue retval]
	: v=QUOTED 					{retval = new StringLiteral($v.text);};
	
integerLiteral returns [GenericValue retval]
	: v=INT						{retval = new IntegerLiteral(Integer.valueOf($v.text));};	

booleanLiteral returns [BooleanValue retval]
	: t=keyTRUE					{retval = new BooleanLiteral($t.text);}
	| f=keyFALSE					{retval = new BooleanLiteral($f.text);}
	;

// Functions
valueFunctions returns [GenericValue retval]
	: keyNOW LPAREN	RPAREN				{retval = new DateConstant(DateConstant.Type.NOW);}
	| keyMINDATE LPAREN RPAREN			{retval = new DateConstant(DateConstant.Type.MINDATE);}
	| keyMAXDATE LPAREN RPAREN			{retval = new DateConstant(DateConstant.Type.MAXDATE);}
	| keyDATE LPAREN s1=valueExpr COMMA s2=valueExpr RPAREN
							{retval = new DateLiteral($s1.retval, $s2.retval);}
	| keyYEAR LPAREN n=valueExpr RPAREN		{retval = new Interval(Interval.Type.YEAR, $n.retval);}
	| keyWEEK LPAREN n=valueExpr RPAREN		{retval = new Interval(Interval.Type.WEEK, $n.retval);}
	| keyDAY LPAREN n=valueExpr RPAREN		{retval = new Interval(Interval.Type.DAY, $n.retval);}
	| keyHOUR LPAREN n=valueExpr RPAREN		{retval = new Interval(Interval.Type.HOUR, $n.retval);}
	| keyMINUTE LPAREN n=valueExpr RPAREN		{retval = new Interval(Interval.Type.MINUTE, $n.retval);}
	| keySECOND LPAREN n=valueExpr RPAREN		{retval = new Interval(Interval.Type.SECOND, $n.retval);}
	| keyMILLI LPAREN n=valueExpr RPAREN		{retval = new Interval(Interval.Type.MILLI, $n.retval);}
	| keyCONCAT LPAREN s1=valueExpr COMMA s2=valueExpr RPAREN
							{retval = new Function(Function.Type.CONCAT, $s1.retval, $s2.retval);}
	| keySUBSTRING LPAREN s=valueExpr COMMA n1=valueExpr COMMA n2=valueExpr RPAREN
							{retval = new Function(Function.Type.SUBSTRING, $s.retval, $n1.retval, $n2.retval);}
	| keyTRIM LPAREN s=valueExpr RPAREN		{retval = new Function(Function.Type.TRIM, $s.retval);}
	| keyLOWER LPAREN s=valueExpr RPAREN		{retval = new Function(Function.Type.LOWER, $s.retval);} 
	| keyUPPER LPAREN s=valueExpr RPAREN		{retval = new Function(Function.Type.UPPER, $s.retval);} 
	| keyREPLACE LPAREN s1=valueExpr COMMA s2=valueExpr COMMA s3=valueExpr RPAREN		
							{retval = new Function(Function.Type.REPLACE, $s1.retval, $s2.retval, $s3.retval);} 
	| keyLENGTH LPAREN s=valueExpr RPAREN		{retval = new Function(Function.Type.LENGTH, $s.retval);}
	| keyINDEXOF LPAREN s1=valueExpr COMMA s2=valueExpr RPAREN
							{retval = new Function(Function.Type.INDEXOF, $s1.retval, $s2.retval);}
	| keyIF v1=booleanExpr keyTHEN v2=valueExpr keyELSE v3=valueExpr keyEND	
							{retval = new DelegateTernary($v1.retval, $v2.retval, $v3.retval);}
	;
			
column 	: c=varRef;
	
schemaDesc returns [Schema retval]
	: LCURLY a=attribList RCURLY			{retval = HUtil.newDefinedSchema(input, $a.retval);};
	
ltgtOp returns [Operator retval]
	: GT 						{$ltgtOp.retval = Operator.GT;}
	| GTEQ 						{$ltgtOp.retval = Operator.GTEQ;}
	| LT 						{$ltgtOp.retval = Operator.LT;}
	| LTEQ 						{$ltgtOp.retval = Operator.LTEQ;}
	;
			
eqneOp returns [Operator retval]
	: EQ EQ?					{$eqneOp.retval = Operator.EQ;}
	| (LTGT | BANGEQ)				{$eqneOp.retval = Operator.NOTEQ;}
	;
				
qstring	: QUOTED ;					

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
