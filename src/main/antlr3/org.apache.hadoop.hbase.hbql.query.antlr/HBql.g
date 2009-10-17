grammar HBql;

options {superClass=ParserSupport;}

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
import org.apache.hadoop.hbase.hbql.query.cmds.*;
import org.apache.hadoop.hbase.hbql.query.antlr.*;
import org.apache.hadoop.hbase.hbql.query.stmt.args.*;
import org.apache.hadoop.hbase.hbql.query.stmt.select.*;
import org.apache.hadoop.hbase.hbql.query.util.*;
import org.apache.hadoop.hbase.hbql.query.schema.*;
import java.util.Date;
}

@lexer::header {
package org.apache.hadoop.hbase.hbql.query.antlr;
import org.apache.hadoop.hbase.hbql.query.util.*;
}

commandStmt returns [ConnectionCmd retval]
options {backtrack=true;}	
	: keyDROP keyTABLE t=simpleName 		{retval = new DropCmd($t.text);}
	| keyDISABLE keyTABLE t=simpleName 		{retval = new DisableCmd($t.text);}
	| keyENABLE keyTABLE t=simpleName 		{retval = new EnableCmd($t.text);}
	| keyDESCRIBE keyTABLE t=simpleName 		{retval = new DescribeCmd($t.text);}
	| keySHOW keyTABLES 		 		{retval = new ShowCmd();}
	| keySET i=simpleName EQ? v=QUOTED	 	{retval = new SetCmd($i.text, $v.text);}
	| cr=createStmt					{retval = $cr.retval;}
	| del=deleteStmt		 		{retval = $del.retval;}
	;

schemaStmt returns [SchemaManagerCmd retval]
	: d=defineStmt 					{retval = $d.retval;}
	;

createStmt returns [CreateCmd retval]
	: keyCREATE keyTABLE keyUSING t=simpleName 	{retval = new CreateCmd($t.text);}
	;
	
defineStmt returns [DefineCmd retval]
	: keyDEFINE keyTABLE t=simpleName (keyALIAS a=simpleName)? LPAREN l=attribList RPAREN
							{retval = new DefineCmd($t.text, $a.text, $l.retval);};

attribList returns [List<ColumnDescription> retval] 
@init {retval = Lists.newArrayList();}
	: (a1=defineAttrib {retval.add($a1.retval);} (COMMA a2=defineAttrib {retval.add($a2.retval);})*)?;
	
defineAttrib returns [ColumnDescription retval]
	: c=varRef type=simpleName (b=LBRACE RBRACE)? m=keyMAP? (keyALIAS a=simpleName)?	
							{retval = ColumnDescription.newColumn($c.text, $a.text, $m.text!=null, false, $type.text, $b.text!=null);}
	| f=familyRef (keyALIAS a=simpleName)?		{retval = ColumnDescription.newFamilyDefault($f.text, $a.text);}
	;


deleteStmt  returns [DeleteCmd retval]
	: keyDELETE keyFROM t=simpleName w=whereValue?			
	  						{retval = new DeleteCmd($t.text, $w.retval);};

selectStmt returns [QueryArgs retval]
	: keySELECT c=selectElems keyFROM t=simpleName w=whereValue?			
							{retval = new QueryArgs($c.retval, $t.text, $w.retval);};

selectElems returns [List<SelectElement> retval]
	: STAR						{retval = FamilySelectElement.newAllFamilies();}
	| c=selectElemList				{retval = $c.retval;}
	;
	
selectElemList returns [List<SelectElement> retval]
@init {retval = Lists.newArrayList();}
	: c1=selectElem {retval.add($c1.retval);} (COMMA c2=selectElem {retval.add($c2.retval);})*;

selectElem returns [SelectElement retval]
	: c=valExpr (keyAS i=simpleName)?		{$selectElem.retval = ExprSelectElement.newExprElement($c.retval, $i.text);}
	| f=familyRef					{$selectElem.retval = FamilySelectElement.newFamilyElement($f.text);}
	;

whereValue returns [WhereArgs retval]
@init {retval = new WhereArgs();}
	: keyWITH selectDesc[retval]+;

selectDesc[WhereArgs whereArgs] 
	: k=keysRange					{whereArgs.setKeyRangeArgs($k.retval);}
	| t=time					{whereArgs.setTimeRangeArgs($t.retval);}	
	| v=versions					{whereArgs.setVersionArgs($v.retval);}
	| l=scanLimit					{whereArgs.setScanLimitArgs($l.retval);}
	| q=queryLimit					{whereArgs.setQueryLimitArgs($q.retval);}
	| s=serverFilter				{whereArgs.setServerExprTree($s.retval);}
	| c=clientFilter				{whereArgs.setClientExprTree($c.retval);}
	;
	
keysRange returns [KeyRangeArgs retval]
	: keyKEYS k=keyRangeList			{retval = new KeyRangeArgs($k.retval);}	
	| keyKEYS keyALL				{retval = new KeyRangeArgs();}	
	;
	
time returns [TimeRangeArgs retval]
	: keyTIME keyRANGE d1=valExpr keyTO d2=valExpr	{retval = new TimeRangeArgs($d1.retval, $d2.retval);}
	| keyTIME keySTAMP d1=valExpr			{retval = new TimeRangeArgs($d1.retval, $d1.retval);}
	;
		
versions returns [VersionArgs retval]
	: keyVERSIONS v=valExpr				{retval = new VersionArgs($v.retval);}
	| keyVERSIONS keyMAX				{retval = new VersionArgs(new IntegerLiteral(Integer.MAX_VALUE));}
	;
	
scanLimit returns [LimitArgs retval]
	: keySCAN keyLIMIT v=valExpr			{retval = new LimitArgs($v.retval);};
	
queryLimit returns [LimitArgs retval]
	: keyQUERY keyLIMIT v=valExpr			{retval = new LimitArgs($v.retval);};
	
clientFilter returns [ExprTree retval]
	: keyCLIENT keyFILTER keyWHERE w=descWhereExpr	
							{retval = $w.retval;};
	
serverFilter returns [ExprTree retval]
	: keySERVER keyFILTER keyWHERE w=descWhereExpr	
							{retval = $w.retval;};
	
keyRangeList returns [List<KeyRangeArgs.Range> retval]
@init {retval = Lists.newArrayList();}
	:  k1=keyRange {retval.add($k1.retval);} (COMMA k2=keyRange {retval.add($k2.retval);})*;
	
keyRange returns [KeyRangeArgs.Range retval]
options {backtrack=true;}	
	: q1=valExpr keyTO keyLAST			{retval = KeyRangeArgs.newLastRange($q1.retval);}
	| q1=valExpr keyTO q2=valExpr			{retval = KeyRangeArgs.newRange($q1.retval, $q2.retval);}
	| q1=valExpr 					{retval = KeyRangeArgs.newSingleKey($q1.retval);}
	;
	
nodescWhereExpr returns [ExprTree retval]
	 : e=boolExpr					{retval = ExprTree.newExprTree($e.retval);};

descWhereExpr returns [ExprTree retval]
	: s=schemaDesc? e=boolExpr			{retval = ExprTree.newExprTree($e.retval); if ($s.retval != null) retval.setSchema($s.retval);};

// Boolean Expressions				
boolExpr returns [BooleanValue retval]
	: e1=andExpr (keyOR e2=boolExpr)?		{$boolExpr.retval = ($e2.text == null) ? $e1.retval : new BooleanCompare($e1.retval, Operator.OR, $e2.retval);};

andExpr returns [BooleanValue retval]
	: e1=booleanNot (keyAND e2=andExpr)?		{$andExpr.retval = ($e2.text == null) ? $e1.retval : new BooleanCompare($e1.retval, Operator.AND, $e2.retval);};

booleanNot returns [BooleanValue retval]			 
	: (n=keyNOT)? p=booleanPrimary			{retval = ($n.text != null) ? new BooleanNot(true, $p.retval) :  $p.retval;};

booleanPrimary returns [BooleanValue retval]
	: b=eqneCompare					{retval = $b.retval;};

eqneCompare returns [BooleanValue retval]
options {backtrack=true;}	
	: v1=valExpr o=eqneOp v2=valExpr 		{retval = new DelegateCompare($v1.retval, $o.retval, $v2.retval);}	
	| c=ltgtCompare					{retval = $c.retval;}
	;

ltgtCompare returns [BooleanValue retval]
options {backtrack=true;}	
	: v1=valExpr o=ltgtOp v2=valExpr		{retval = new DelegateCompare($v1.retval, $o.retval, $v2.retval);}
	| p=booleanParen				{retval = $p.retval;}
	;

booleanParen returns [BooleanValue retval]
options {backtrack=true;}	
	: LPAREN o=boolExpr RPAREN			{retval = $o.retval;}
	| b=booleanVal					{retval = $b.retval;}
	;

booleanVal returns [BooleanValue retval]
options {backtrack=true;}	
	: f=booleanFuncs				{retval = $f.retval;}
	| b=booleanAtom					{retval = new BooleanExpr($b.retval);}
	;
	
booleanAtom returns [GenericValue retval]
	: b=booleanLiteral				{retval = $b.retval;}
	| v=varRef					{retval = new DelegateColumn($v.text);}
	| p=paramRef					{retval = new NamedParameter($p.text);}
	;

booleanFuncs returns [BooleanValue retval]
options {backtrack=true; memoize=true;}	
	: s1=valExpr n=keyNOT? keyCONTAINS s2=valExpr		
							{retval = new ContainsStmt($s1.retval, ($n.text != null), $s2.retval);}
	| s1=valExpr n=keyNOT? keyLIKE s2=valExpr 	{retval = new LikeStmt($s1.retval, ($n.text != null), $s2.retval);}
	| s1=valExpr n=keyNOT? keyBETWEEN s2=valExpr keyAND s3=valExpr		
							{retval = new DelegateBetweenStmt($s1.retval, ($n.text != null), $s2.retval, $s3.retval);}
	| s1=valExpr n=keyNOT? keyIN LPAREN l=valueItemList RPAREN			
							{retval = new DelegateInStmt($s1.retval, ($n.text != null), $l.retval);} 
	| s1=valExpr keyIS (n=keyNOT)? keyNULL		{retval = new DelegateNullCompare(($n.text != null), $s1.retval);}	
	| keyDEFINEDINROW LPAREN s=valExpr RPAREN	{retval = new BooleanFunction(Function.Type.DEFINEDINROW, $s.retval);}
	;

valueItemList returns [List<GenericValue> retval]
@init {retval = Lists.newArrayList();}
	: i1=valExpr {retval.add($i1.retval);} (COMMA i2=valExpr {retval.add($i2.retval);})*;
	
// Value Expressions
valExpr returns [GenericValue retval] 
options {backtrack=true; memoize=true;}	
	: v=valuePrimary				{retval = $v.retval;}
	| LPAREN o=boolExpr RPAREN			{retval = $o.retval;}
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
	: (s=plusMinus)? n=parenExpr 			{$signedExpr.retval = ($s.retval == Operator.MINUS) ? new DelegateCalculation($n.retval, Operator.NEGATIVE, new IntegerLiteral(0)) : $n.retval;};

parenExpr returns [GenericValue retval]
options {backtrack=true; memoize=true;}	
	: n=atomExpr					{retval = $n.retval;}
	| LPAREN s=valExpr RPAREN			{retval = $s.retval;}
	;
	   						 
atomExpr returns [GenericValue retval]
	: v=valueAtom					{retval = $v.retval;} 
	| f=valueFunctions				{retval = $f.retval;}
	;

// Value Atom
valueAtom returns [GenericValue retval]
	: s=stringLiteral				{retval = $s.retval;}
	| i=integerLiteral				{retval = $i.retval;}
	| l=longLiteral					{retval = $l.retval;}
	| d=doubleLiteral				{retval = $d.retval;}
	| b=booleanAtom					{retval = $b.retval;}
	| keyNULL					{retval = new StringNullLiteral();}
	;

// Literals		
stringLiteral returns [GenericValue retval]
	: v=QUOTED 					{retval = new StringLiteral($v.text);};
	
integerLiteral returns [GenericValue retval]
	: v=INT						{retval = new IntegerLiteral(Integer.valueOf($v.text));};	

longLiteral returns [GenericValue retval]
	: v=LONG					{retval = new LongLiteral(Long.valueOf($v.text.substring(0, $v.text.length()-1)));};	

doubleLiteral returns [GenericValue retval]
	: v=DOUBLE					{retval = new DoubleLiteral(Double.valueOf($v.text));};	

booleanLiteral returns [BooleanValue retval]
	: t=keyTRUE					{retval = new BooleanLiteral($t.text);}
	| f=keyFALSE					{retval = new BooleanLiteral($f.text);}
	;

// Functions
valueFunctions returns [GenericValue retval]
	: keyNOW LPAREN	RPAREN				{retval = new DateLiteral(DateLiteral.Type.NOW);}
	| keyMINDATE LPAREN RPAREN			{retval = new DateLiteral(DateLiteral.Type.MINDATE);}
	| keyMAXDATE LPAREN RPAREN			{retval = new DateLiteral(DateLiteral.Type.MAXDATE);}
	| keyDATE LPAREN s1=valExpr COMMA s2=valExpr RPAREN
							{retval = new DateString($s1.retval, $s2.retval);}
	| keyYEAR LPAREN n=valExpr RPAREN		{retval = new Interval(Interval.Type.YEAR, $n.retval);}
	| keyWEEK LPAREN n=valExpr RPAREN		{retval = new Interval(Interval.Type.WEEK, $n.retval);}
	| keyDAY LPAREN n=valExpr RPAREN		{retval = new Interval(Interval.Type.DAY, $n.retval);}
	| keyHOUR LPAREN n=valExpr RPAREN		{retval = new Interval(Interval.Type.HOUR, $n.retval);}
	| keyMINUTE LPAREN n=valExpr RPAREN		{retval = new Interval(Interval.Type.MINUTE, $n.retval);}
	| keySECOND LPAREN n=valExpr RPAREN		{retval = new Interval(Interval.Type.SECOND, $n.retval);}
	| keyMILLI LPAREN n=valExpr RPAREN		{retval = new Interval(Interval.Type.MILLI, $n.retval);}

	| keyCONCAT LPAREN s1=valExpr COMMA s2=valExpr RPAREN
							{retval = new StringFunction(Function.Type.CONCAT, $s1.retval, $s2.retval);}
	| keySUBSTRING LPAREN s=valExpr COMMA n1=valExpr COMMA n2=valExpr RPAREN
							{retval = new StringFunction(Function.Type.SUBSTRING, $s.retval, $n1.retval, $n2.retval);}
	| keyTRIM LPAREN s=valExpr RPAREN		{retval = new StringFunction(Function.Type.TRIM, $s.retval);}
	| keyLOWER LPAREN s=valExpr RPAREN		{retval = new StringFunction(Function.Type.LOWER, $s.retval);} 
	| keyUPPER LPAREN s=valExpr RPAREN		{retval = new StringFunction(Function.Type.UPPER, $s.retval);} 
	| keyREPLACE LPAREN s1=valExpr COMMA s2=valExpr COMMA s3=valExpr RPAREN		
							{retval = new StringFunction(Function.Type.REPLACE, $s1.retval, $s2.retval, $s3.retval);} 

	| keyLENGTH LPAREN s=valExpr RPAREN		{retval = new NumericFunction(Function.Type.LENGTH, $s.retval);}
	| keyINDEXOF LPAREN s1=valExpr COMMA s2=valExpr RPAREN
							{retval = new NumericFunction(Function.Type.INDEXOF, $s1.retval, $s2.retval);}
	| keySHORT LPAREN s=valExpr RPAREN		{retval = new NumericFunction(Function.Type.SHORT, $s.retval);}
	| keyINTEGER LPAREN s=valExpr RPAREN		{retval = new NumericFunction(Function.Type.INTEGER, $s.retval);}
	| keyLONG LPAREN s=valExpr RPAREN		{retval = new NumericFunction(Function.Type.LONG, $s.retval);}
	| keyFLOAT LPAREN s=valExpr RPAREN		{retval = new NumericFunction(Function.Type.FLOAT, $s.retval);}
	| keyDOUBLE LPAREN s=valExpr RPAREN		{retval = new NumericFunction(Function.Type.DOUBLE, $s.retval);}
	| keyIF v1=boolExpr keyTHEN v2=valExpr keyELSE v3=valExpr keyEND	
							{retval = new DelegateTernary($v1.retval, $v2.retval, $v3.retval);}
	;
			
schemaDesc returns [Schema retval]
	: LCURLY a=attribList RCURLY			{retval = newDefinedSchema(input, $a.retval);};
	
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

simpleName : ID;
varRef 	: ID (COLON ID)?;
familyRef : ID COLON STAR;	
paramRef: COLON ID;
		
INT	: DIGIT+;
LONG	: DIGIT+'L';
DOUBLE	: DIGIT+ DOT DIGIT*;

ID : CHAR (CHAR | DOT | MINUS | DOLLAR | DIGIT)*; // DOLLAR is for inner class table names
	 
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
keyAS		: {isKeyword(input, "AS")}? ID;
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
keyDEFINEDINROW : {isKeyword(input, "DEFINEDINROW")}? ID;
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
keySHORT	: {isKeyword(input, "SHORT")}? ID;
keyINTEGER	: {isKeyword(input, "INTEGER")}? ID;
keyLONG		: {isKeyword(input, "LONG")}? ID;
keyFLOAT	: {isKeyword(input, "FLOAT")}? ID;
keyDOUBLE	: {isKeyword(input, "DOUBLE")}? ID;
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
keyMAP		: {isKeyword(input, "MAPKEYSASCOLUMNS")}? ID;
