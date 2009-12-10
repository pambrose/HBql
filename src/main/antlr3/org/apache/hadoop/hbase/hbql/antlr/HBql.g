/*
 * Copyright (c) 2009.  The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
grammar HBql;

options {superClass=ParserSupport;}

tokens {
	SEMI = ';';
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

@lexer::members {
  public void reportError(RecognitionException e) {
    throw new LexerRecognitionException(e, e.getMessage());
  }
}
@rulecatch {catch (RecognitionException re) {handleRecognitionException(re);}}

@header {
package org.apache.hadoop.hbase.hbql.antlr;

import org.apache.hadoop.hbase.hbql.parser.*;
import org.apache.hadoop.hbase.hbql.statement.*;
import org.apache.hadoop.hbase.hbql.statement.args.*;
import org.apache.hadoop.hbase.hbql.statement.select.*;
import org.apache.hadoop.hbase.hbql.mapping.*;
import org.apache.hadoop.hbase.hbql.mapping.property.*;

import org.apache.expreval.expr.*;
import org.apache.expreval.expr.node.*;
import org.apache.expreval.expr.betweenstmt.*;
import org.apache.expreval.expr.calculation.*;
import org.apache.expreval.expr.casestmt.*;
import org.apache.expreval.expr.compare.*;
import org.apache.expreval.expr.function.*;
import org.apache.expreval.expr.ifthenstmt.*;
import org.apache.expreval.expr.instmt.*;
import org.apache.expreval.expr.literal.*;
import org.apache.expreval.expr.nullcomp.*;
import org.apache.expreval.expr.stringpattern.*;
import org.apache.expreval.expr.var.*;
import org.apache.expreval.util.*;
}

@lexer::header {
package org.apache.hadoop.hbase.hbql.antlr;
import org.apache.expreval.client.*;
import org.apache.hadoop.hbase.hbql.parser.*;
}

consoleStatements returns [List<HBqlStatement> retval]
@init {retval = Lists.newArrayList();}
	: c1=consoleStatement SEMI {retval.add($c1.retval);} ((c2=consoleStatement {retval.add($c2.retval);})? SEMI )*;
	
consoleStatement returns [HBqlStatement retval]
options {backtrack=true;}	
	: keySHOW keyTABLES 		 		{retval = new ShowTablesStatement();}
	| keySHOW keyMAPPINGS 		 		{retval = new ShowMappingsStatement();}
	| keyIMPORT val=QSTRING				{retval = new ImportStatement($val.text);}
	| keyPARSE c=consoleStatement			{retval = new ParseStatement($c.retval);}
	| keyEVAL te=exprValue				{retval = new EvalStatement($te.retval);}
	| keySET t=simpleId EQ? val=QSTRING	 	{retval = new SetStatement($t.text, $val.text);}
	| keyVERSION					{retval = new VersionStatement();}
	| keyHELP					{retval = new HelpStatement();}
	| jdbc=jdbcStatement				{retval = $jdbc.retval;}
	;						

jdbcStatement returns [HBqlStatement retval]
options {backtrack=true;}	
	: sel=selectStatement				{retval = $sel.retval;}			
	| keyDELETE di=deleteItemList? keyFROM keyMAPPING? t=simpleId w=withClause? p=pred?	
							{retval = new DeleteStatement($p.retval, $di.retval, $t.text, $w.retval);}
	| keyINSERT keyINTO keyMAPPING? t=simpleId LPAREN e=exprList RPAREN ins=insertValues p=pred?
							{retval = new InsertStatement($p.retval, $t.text, $e.retval, $ins.retval);}
	| keyCREATE (tmp=keyTEMP)? keyMAPPING t=simpleId (keyFOR keyTABLE a=simpleId)? (am=attribMapping)? p=pred? 
							{retval = new CreateMappingStatement($p.retval, $tmp.text != null,$t.text, $a.text, $am.retval);}
	| keyDROP keyMAPPING t=simpleId p=pred?		{retval = new DropMappingStatement($p.retval, $t.text);}
	| keyDESCRIBE keyMAPPING t=simpleId 		{retval = new DescribeMappingStatement($t.text);}
	| keyCREATE keyTABLE t=simpleId LPAREN fd=familyDefinitionList RPAREN p=pred?
							{retval = new CreateTableStatement($p.retval, $t.text, $fd.retval);}
	| keyDESCRIBE keyTABLE t=simpleId 		{retval = new DescribeTableStatement($t.text);}
	| keyDROP keyTABLE t=simpleId p=pred? 		{retval = new DropTableStatement($p.retval, $t.text);}
	| keyALTER keyTABLE t=simpleId aal=alterActionList p=pred?	
							{retval = new AlterTableStatement($p.retval, $t.text, $aal.retval);}
	| keyDISABLE keyTABLE t=simpleId p=pred?	{retval = new DisableTableStatement($p.retval, $t.text);}
	| keyENABLE keyTABLE t=simpleId p=pred?		{retval = new EnableTableStatement($p.retval, $t.text);}
	| keyCREATE keyINDEX t=simpleId keyON keyMAPPING? t2=simpleId LPAREN t3=indexColumnList RPAREN (keyINCLUDE LPAREN t4=indexColumnList RPAREN)? p=pred?		
							{retval = new CreateIndexStatement($p.retval, $t.text, $t2.text, $t3.retval, $t4.retval);}
	| keyDROP keyINDEX t=simpleId keyON t2=simpleId p=pred?		
							{retval = new DropIndexStatement($p.retval, $t.text, $t2.text);}
	;

indexColumnList returns [List<String> retval]
@init {retval = Lists.newArrayList();}
	: a1=indexColumn {retval.add($a1.text);} (COMMA a2=indexColumn {retval.add($a2.text);})*;

indexColumn 
	: columnRef
	| familyWildCard;
		
attribMapping returns [AttribMapping retval]
	: LPAREN key=simpleId keyKEY (COMMA fm=familyMappingList)? RPAREN
							{retval = new AttribMapping($key.text, $fm.retval);};
	
pred returns [StatementPredicate retval]
	: keyIF  b=exprValue 				{retval = new StatementPredicate($b.retval);};

	
alterActionList returns [List<AlterTableAction> retval]
@init {retval = Lists.newArrayList();}
	: a1=alterAction {retval.add($a1.retval);} (COMMA a2=alterAction {retval.add($a2.retval);})*;

alterAction returns [AlterTableAction retval]
options {backtrack=true;}	
	: keyALTER keyFAMILY t=simpleId  keyTO fd=familyDefinition	
							{retval = new AlterFamilyAction($t.text, $fd.retval);}
	| keyDROP keyFAMILY t=simpleId			{retval = new DropFamilyAction($t.text);}
	| keyADD keyFAMILY  fd=familyDefinition 	{retval = new AddFamilyAction($fd.retval);}
	;
		
deleteItemList returns [List<String> retval]
@init {retval = Lists.newArrayList();}
	: a1=deleteItem {retval.add($a1.text);} (COMMA a2=deleteItem {retval.add($a2.text);})*;

deleteItem 
	: columnRef
	| familyWildCard;
	
insertValues returns [InsertValueSource retval]
	: keyVALUES LPAREN e=insertExprList RPAREN	{retval = new InsertSingleRow($e.retval);}
	| sel=selectStatement				{retval = new InsertSelectValues($sel.retval);};
			
selectStatement returns [SelectStatement retval]
	: keySELECT c=selectElems keyFROM keyMAPPING? t=simpleId w=withClause?
							{retval = new SelectStatement($c.retval, $t.text, $w.retval);};
							
familyDefinitionList returns [List<FamilyDefinition> retval]
@init {retval = Lists.newArrayList();}
	: a1=familyDefinition {retval.add($a1.retval);} (COMMA a2=familyDefinition {retval.add($a2.retval);})*;

familyDefinition returns [FamilyDefinition retval]
	: t=simpleId LPAREN p=familyPropertyList? RPAREN	
							{retval = new FamilyDefinition($t.text, $p.retval);};

familyPropertyList returns [List<FamilyProperty> retval]							
@init {retval = Lists.newArrayList();}
	: a1=familyProperty {retval.add($a1.retval);} (COMMA a2=familyProperty {retval.add($a2.retval);})*;
	
familyProperty returns [FamilyProperty retval]
	: keyMAX_VERSIONS COLON v=exprValue		{retval = new MaxVersionsProperty($v.retval);}
	| keyBLOOM_FILTER COLON v=exprValue		{retval = new BloomFilterProperty($v.retval);}
	| keyBLOCK_SIZE COLON v=exprValue		{retval = new BlockSizeProperty($v.retval);}
	| keyBLOCK_CACHE_ENABLED COLON v=exprValue	{retval = new BlockCacheProperty($v.retval);}
	| keyCOMPRESSION_TYPE COLON c=compressionType	{retval = new CompressionTypeProperty($c.text);}
	| keyIN_MEMORY COLON v=exprValue		{retval = new InMemoryProperty($v.retval);}
	| keyMAP_FILE_INDEX_INTERVAL COLON v=exprValue	{retval = new MapFileIndexIntervalProperty($v.retval);}
	| keyTTL COLON v=exprValue			{retval = new TtlProperty($v.retval);}
	;

compressionType
	: keyGZ | keyLZO | keyNONE;

familyMappingList returns [List<FamilyMapping> retval]
@init {retval = Lists.newArrayList();}
	: a1=familyMapping {retval.add($a1.retval);} (COMMA a2=familyMapping {retval.add($a2.retval);})*;

familyMapping returns [FamilyMapping retval]
	: f=simpleId (keyINCLUDE d=keyUNMAPPED)? (LPAREN c=columnDefinitionnList RPAREN)? 
							{retval = new FamilyMapping($f.text,  $d.text!=null, $c.retval);};

columnDefinitionnList returns [List<ColumnDefinition> retval]
@init {retval = Lists.newArrayList();}
	: a1=columnDefinition {retval.add($a1.retval);} (COMMA a2=columnDefinition {retval.add($a2.retval);})*;
							
columnDefinition returns [ColumnDefinition retval]
	: s=simpleId type=simpleId (b=LBRACE RBRACE)? (keyALIAS a=simpleId)? (keyDEFAULT def=exprValue)?
							{retval = ColumnDefinition.newMappedColumn($s.text, $type.text, $b.text!=null, $a.text, $def.retval);};
								 
withClause returns [WithArgs retval]
@init {retval = new WithArgs();}
	: keyWITH withElements[retval]+;

withElements[WithArgs withArgs] 
	: k=keysRangeArgs				{withArgs.setKeyRangeArgs($k.retval);}
	| keyINDEX idx=simpleId				{withArgs.setIndexName($idx.text);}	
	| t=timestampArgs				{withArgs.setTimestampArgs($t.retval);}	
	| v=versionArgs					{withArgs.setVersionArgs($v.retval);}
	| sc=scannerCacheArgs				{withArgs.setScannerCacheArgs($sc.retval);}
	| l=limitArgs					{withArgs.setLimitArgs($l.retval);}
	| s=serverFilter				{withArgs.setServerExpressionTree($s.retval);}
	| c=clientFilter				{withArgs.setClientExpressionTree($c.retval);}
	;
	
keysRangeArgs returns [KeyRangeArgs retval]
	: keyKEYS k=rangeList				{retval = new KeyRangeArgs($k.retval);}	
	| keyKEYS keyALL				{retval = new KeyRangeArgs();}	
	;

rangeList returns [List<KeyRangeArgs.Range> retval]
@init {retval = Lists.newArrayList();}
	: k1=keyRange {retval.add($k1.retval);} (COMMA k2=keyRange {retval.add($k2.retval);})*;
	
keyRange returns [KeyRangeArgs.Range retval]
options {backtrack=true;}	
	: q1=exprValue keyTO keyLAST			{retval = KeyRangeArgs.newLastRange($q1.retval);}
	| keyFIRST keyTO q1=exprValue			{retval = KeyRangeArgs.newFirstRange($q1.retval);}
	| q1=exprValue keyTO q2=exprValue		{retval = KeyRangeArgs.newRange($q1.retval, $q2.retval);}
	| q1=exprValue 					{retval = KeyRangeArgs.newSingleKey($q1.retval);}
	;
		
timestampArgs returns [TimestampArgs retval]
	: keyTIMESTAMP keyRANGE d1=exprValue keyTO d2=exprValue	
							{retval = new TimestampArgs($d1.retval, $d2.retval);}
	| keyTIMESTAMP d1=exprValue			{retval = new TimestampArgs($d1.retval);}
	;
		
versionArgs returns [VersionArgs retval]
	: keyVERSIONS v=exprValue			{retval = new VersionArgs($v.retval);}
	| keyVERSIONS keyMAX				{retval = new VersionArgs(new IntegerLiteral(Integer.MAX_VALUE));}
	;
	
scannerCacheArgs returns [ScannerCacheArgs retval]
	: keySCANNER_CACHE_SIZE v=exprValue		{retval = new ScannerCacheArgs($v.retval);}
	;
	
limitArgs returns [LimitArgs retval]
	: keyLIMIT v=exprValue				{retval = new LimitArgs($v.retval);};
		
clientFilter returns [ExpressionTree retval]
	: keyCLIENT keyFILTER keyWHERE w=nodescWhereExpr	
							{retval = $w.retval;};
	
serverFilter returns [ExpressionTree retval]
	: keySERVER keyFILTER keyWHERE w=nodescWhereExpr	
							{retval = $w.retval;};
	
nodescWhereExpr returns [ExpressionTree retval]
	 : e=exprValue					{retval = ExpressionTree.newExpressionTree(null, $e.retval);};

descWhereExpr returns [ExpressionTree retval]
	: s=mappingDesc? e=exprValue			{retval = ExpressionTree.newExpressionTree($s.retval, $e.retval);};

// Expressions
exprValue returns [GenericValue retval]
	: o=orExpr					{retval = $o.retval;};
				
orExpr returns [GenericValue retval]
@init {List<GenericValue> exprList = Lists.newArrayList(); List<Operator> opList = Lists.newArrayList(); }
	: e1=andExpr {exprList.add($e1.retval);} (keyOR e2=andExpr {opList.add(Operator.OR); exprList.add($e2.retval);})* 
							{retval = getLeftAssociativeBooleanCompare(exprList, opList);};

andExpr returns [GenericValue retval]
@init {List<GenericValue> exprList = Lists.newArrayList(); List<Operator> opList = Lists.newArrayList(); }
	: e1=notExpr {exprList.add($e1.retval);} (keyAND e2=notExpr {opList.add(Operator.AND); exprList.add($e2.retval);})* 
							{retval = getLeftAssociativeBooleanCompare(exprList, opList);};

notExpr returns [GenericValue retval]			 
	: (n=keyNOT)? p=eqneExpr			{retval = ($n.text != null) ? new BooleanNot(true, $p.retval) : $p.retval;};

eqneExpr returns [GenericValue retval]
options {backtrack=true; memoize=true;}	
	: v1=ltgtExpr o=eqneOp v2=ltgtExpr 		{retval = new DelegateCompare($v1.retval, $o.retval, $v2.retval);}	
	| c=ltgtExpr					{retval = $c.retval;}
	;

ltgtExpr returns [GenericValue retval]
options {backtrack=true; memoize=true;}	
	: v1=calcExpr o=ltgtOp v2=calcExpr		{retval = new DelegateCompare($v1.retval, $o.retval, $v2.retval);}
	| b=booleanFunctions				{retval = $b.retval;}
	| p=calcExpr					{retval = $p.retval;}
	;

// Value Expressions
calcExpr returns [GenericValue retval] 
@init {List<GenericValue> exprList = Lists.newArrayList(); List<Operator> opList = Lists.newArrayList(); }
	: e1=multExpr {exprList.add($e1.retval);} (op=plusMinus e2=multExpr {opList.add($op.retval); exprList.add($e2.retval);})*	
							{retval = getLeftAssociativeCalculation(exprList, opList);};
	
multExpr returns [GenericValue retval]
@init {List<GenericValue> exprList = Lists.newArrayList(); List<Operator> opList = Lists.newArrayList(); }
	: e1=signedExpr {exprList.add($e1.retval);} (op=multDiv e2=signedExpr {opList.add($op.retval); exprList.add($e2.retval);})*	
							{retval = getLeftAssociativeCalculation(exprList, opList);};
	
signedExpr returns [GenericValue retval]
	: (s=plusMinus)? n=parenExpr 			{retval = ($s.retval == Operator.MINUS) ? new DelegateCalculation($n.retval, Operator.NEGATIVE, new IntegerLiteral(0)) : $n.retval;};

// The order here is important.  atomExpr has to come after valueFunctions to avoid simpleName conflict
parenExpr returns [GenericValue retval]
options {backtrack=true; memoize=true;}	
	: f=valueFunctions				{retval = $f.retval;}
	| n=atomExpr					{retval = $n.retval;}
	| LPAREN s=exprValue RPAREN			{retval = $s.retval;}
	;
	   						 
atomExpr returns [GenericValue retval]
	: s=stringLiteral				{retval = $s.retval;}
	| i=integerLiteral				{retval = $i.retval;}
	| l=longLiteral					{retval = $l.retval;}
	| d=doubleLiteral				{retval = $d.retval;}
	| b=booleanLiteral				{retval = $b.retval;}
	| keyNULL					{retval = new StringNullLiteral();}
	| p=paramRef					{retval = new NamedParameter($p.text);}
	| v=columnRef					{retval = new DelegateColumn($v.text);}
	;

// Literals		
stringLiteral returns [StringLiteral retval]
	: v=QSTRING 					{retval = new StringLiteral($v.text);};
	
integerLiteral returns [IntegerLiteral retval]
	: v=INT						{retval = new IntegerLiteral($v.text);};	

longLiteral returns [LongLiteral retval]
	: v=LONG					{retval = new LongLiteral($v.text);};	


doubleLiteral returns [GenericValue retval]
	: v1=DOUBLE1					{retval = DoubleLiteral.valueOf($v1.text);}
	| v2=DOUBLE2					{retval = DoubleLiteral.valueOf($v2.text);}
	;	

booleanLiteral returns [BooleanLiteral retval]
	: t=keyTRUE					{retval = new BooleanLiteral($t.text);}
	| f=keyFALSE					{retval = new BooleanLiteral($f.text);}
	;

// Functions
booleanFunctions returns [BooleanValue retval]
options {backtrack=true; memoize=true;}	
	: s1=calcExpr n=keyNOT? keyCONTAINS s2=calcExpr		
							{retval = new ContainsStmt($s1.retval, ($n.text != null), $s2.retval);}
	| s1=calcExpr n=keyNOT? keyLIKE s2=calcExpr 	{retval = new LikeStmt($s1.retval, ($n.text != null), $s2.retval);}
	| s1=calcExpr n=keyNOT? keyBETWEEN s2=calcExpr keyAND s3=calcExpr		
							{retval = new DelegateBetweenStmt($s1.retval, ($n.text != null), $s2.retval, $s3.retval);}
	| s1=calcExpr n=keyNOT? keyIN LPAREN l=exprList RPAREN			
							{retval = new DelegateInStmt($s1.retval, ($n.text != null), $l.retval);} 
	| s1=calcExpr keyIS n=keyNOT? keyNULL		{retval = new DelegateNullCompare(($n.text != null), $s1.retval);}	
	;

valueFunctions returns [GenericValue retval]
options {backtrack=true; memoize=true;}	
	: keyIF t1=exprValue keyTHEN t2=exprValue keyELSE t3=exprValue keyEND	
							{retval = new DelegateIfThen($t1.retval, $t2.retval, $t3.retval);}
	
	| c=caseStmt					{retval = $c.retval;} 						

	| t=simpleId LPAREN a=exprList? RPAREN		{retval = new DelegateFunction($t.text, $a.retval);}
	;

caseStmt returns [DelegateCase retval]
	: keyCASE 					{retval = new DelegateCase();}
	   whenItem[retval]+
	   (keyELSE t=exprValue)? 			{retval.addElse($t.retval);}
	  keyEND
	;
	
whenItem [DelegateCase stmt] 
	: keyWHEN t1=exprValue keyTHEN t2=exprValue	{stmt.addWhen($t1.retval, $t2.retval);};

selectElems returns [List<SelectElement> retval]
	: STAR						{retval = FamilySelectElement.newAllFamilies();}
	| c=selectElemList				{retval = $c.retval;}
	;
	
selectElemList returns [List<SelectElement> retval]
@init {retval = Lists.newArrayList();}
	: c1=selectElem {retval.add($c1.retval);} (COMMA c2=selectElem {retval.add($c2.retval);})*;

selectElem returns [SelectElement retval]
options {backtrack=true; memoize=true;}	
	: b=exprValue (keyAS i2=simpleId)?		{retval = SelectExpressionContext.newExpression($b.retval, $i2.text);}
	| f=familyWildCard				{retval = FamilySelectElement.newFamilyElement($f.text);}
	;

exprList returns [List<GenericValue> retval]
@init {retval = Lists.newArrayList();}
	: i1=exprValue {retval.add($i1.retval);} (COMMA i2=exprValue {retval.add($i2.retval);})*;
				
insertExprList returns [List<GenericValue> retval]
@init {retval = Lists.newArrayList();}
	: i1=insertExpr {retval.add($i1.retval);} (COMMA i2=insertExpr {retval.add($i2.retval);})*;

insertExpr returns [GenericValue retval]
	: t=exprValue					{retval = $t.retval;} 
	| keyDEFAULT					{retval = new DefaultKeyword();}
	;
					
mappingDesc returns [Mapping retval]
	: LCURLY a=attribList RCURLY			{retval = newTableMapping(input, $a.retval);};

attribList returns [List<ColumnDefinition> retval] 
@init {retval = Lists.newArrayList();}
	: a1=attribDesc {retval.add($a1.retval);} (COMMA a2=attribDesc {retval.add($a2.retval);})*;

attribDesc returns [ColumnDefinition retval]
	: c=columnRef type=simpleId (b=LBRACE RBRACE)? (keyALIAS a=simpleId)? (keyDEFAULT t=exprValue)?	
							{retval = ColumnDefinition.newMappedColumn($c.text, $type.text,  $b.text!=null, $a.text, $t.retval);};
		
ltgtOp returns [Operator retval]
	: GT 						{retval = Operator.GT;}
	| GTEQ 						{retval = Operator.GTEQ;}
	| LT 						{retval = Operator.LT;}
	| LTEQ 						{retval = Operator.LTEQ;}
	;
			
eqneOp returns [Operator retval]
	: EQ EQ?					{retval = Operator.EQ;}
	| (LTGT | BANGEQ)				{retval = Operator.NOTEQ;}
	;
				
qstring	: QSTRING ;					

plusMinus returns [Operator retval]
	: PLUS						{retval = Operator.PLUS;}
	| MINUS						{retval = Operator.MINUS;}
	;
	
multDiv returns [Operator retval]
	: STAR						{retval = Operator.MULT;}
	| DIV						{retval = Operator.DIV;}
	| MOD						{retval = Operator.MOD;}
	;

simpleId
 	: ID;
 	
columnRef 
	: ID (COLON ID)?;
		
familyWildCard 
	: ID COLON STAR;
	
paramRef
	: COLON ID
	| QMARK;
		
INT	: DIGIT+;
LONG	: DIGIT+ ('L' | 'l');
DOUBLE1	: DIGIT+ (DOT DIGIT*)? ('D' | 'd' | 'F' | 'f');
DOUBLE2	: DIGIT+ DOT DIGIT*;

ID : CHAR (CHAR | DOT | MINUS | DOLLAR | DIGIT)*; // DOLLAR is for inner class table names

fragment
DIGIT	: '0'..'9'; 

fragment
CHAR 	: 'a'..'z' | 'A'..'Z' | '_'; 
	 
QSTRING		
@init {final StringBuilder sbuf = new StringBuilder();}	
	: DQUOTE (options {greedy=false;} : any=DQCHAR {sbuf.append(ParserSupport.decodeEscapedChar($any.getText()));})* DQUOTE {setText(sbuf.toString());}
	| SQUOTE (options {greedy=false;} : any=SQCHAR {sbuf.append(ParserSupport.decodeEscapedChar($any.getText()));})* SQUOTE {setText(sbuf.toString());}
	;

fragment 
DQCHAR
    : ESC_SEQ | ~('\\'|'\"');
    	
fragment 
SQCHAR
    : ESC_SEQ | ~('\\'|'\'');
    	
fragment
ESC_SEQ
    :   '\\' ('b'|'t'|'n'|'f'|'r'|'\"'|'\''|'\\')
    |   UNICODE_ESC
    |   OCTAL_ESC
    ;

fragment
OCTAL_ESC
    :   '\\' ('0'..'3') ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7')
    ;

fragment
UNICODE_ESC
    :   '\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
    ;

fragment
HEX_DIGIT : ('0'..'9'|'a'..'f'|'A'..'F') ;

COMMENT
    :   '//' ~('\n'|'\r')* '\r'? '\n' {skip();}
    |   '/*' ( options {greedy=false;} : . )* '*/' {skip();}
    ;

WS 	: (' ' |'\t' |'\n' |'\r' )+ {skip();};

keyALTER 	: {isKeyword(input, "ALTER")}? ID;
keyINDEX 	: {isKeyword(input, "INDEX")}? ID;
keyON	 	: {isKeyword(input, "ON")}? ID;
keyADD	 	: {isKeyword(input, "ADD")}? ID;
keyFAMILY 	: {isKeyword(input, "FAMILY")}? ID;
keySELECT 	: {isKeyword(input, "SELECT")}? ID;
keyDELETE 	: {isKeyword(input, "DELETE")}? ID;
keyCREATE 	: {isKeyword(input, "CREATE")}? ID;
keyDESCRIBE 	: {isKeyword(input, "DESCRIBE")}? ID;
keySHOW 	: {isKeyword(input, "SHOW")}? ID;
keyENABLE 	: {isKeyword(input, "ENABLE")}? ID;
keyDISABLE 	: {isKeyword(input, "DISABLE")}? ID;
keyDROP 	: {isKeyword(input, "DROP")}? ID;
keyTABLE 	: {isKeyword(input, "TABLE")}? ID;
keyMAPPING 	: {isKeyword(input, "MAPPING")}? ID;
keyMAPPINGS 	: {isKeyword(input, "MAPPINGS")}? ID;
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
keyFIRST	: {isKeyword(input, "FIRST")}? ID;
keyLAST		: {isKeyword(input, "LAST")}? ID;
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
keyFOR	 	: {isKeyword(input, "FOR")}? ID;
keyCLIENT	: {isKeyword(input, "CLIENT")}? ID;
keySERVER	: {isKeyword(input, "SERVER")}? ID;
keyVERSIONS	: {isKeyword(input, "VERSIONS")}? ID;
keyVERSION	: {isKeyword(input, "VERSION")}? ID;
keyTIMESTAMP	: {isKeyword(input, "TIMESTAMP")}? ID;
keyRANGE	: {isKeyword(input, "RANGE")}? ID;
keyMAX		: {isKeyword(input, "MAX")}? ID;
keyKEYS		: {isKeyword(input, "KEYS")}? ID;
keyALL		: {isKeyword(input, "ALL")}? ID;
keyCONTAINS	: {isKeyword(input, "CONTAINS")}? ID;
keyDEFAULT	: {isKeyword(input, "DEFAULT")}? ID;
keyCASE		: {isKeyword(input, "CASE")}? ID;
keyWHEN		: {isKeyword(input, "WHEN")}? ID;
keyINSERT	: {isKeyword(input, "INSERT")}? ID;
keyINTO		: {isKeyword(input, "INTO")}? ID;
keyVALUES	: {isKeyword(input, "VALUES")}? ID;
keyHELP		: {isKeyword(input, "HELP")}? ID;
keyPARSE	: {isKeyword(input, "PARSE")}? ID;
keyEVAL		: {isKeyword(input, "EVAL")}? ID;
keyIMPORT	: {isKeyword(input, "IMPORT")}? ID;
keyTEMP		: {isKeyword(input, "TEMP")}? ID;
keyTTL		: {isKeyword(input, "TTL")}? ID;
keyGZ		: {isKeyword(input, "GZ")}? ID;
keyLZO		: {isKeyword(input, "LZO")}? ID;
keyNONE		: {isKeyword(input, "NONE")}? ID;
keyINCLUDE	: {isKeyword(input, "INCLUDE")}? ID;
keyUNMAPPED	: {isKeyword(input, "UNMAPPED")}? ID;
keyKEY		: {isKeyword(input, "KEY")}? ID;
keyMAX_VERSIONS	: {isKeyword(input, "MAX_VERSIONS")}? ID;
keyBLOOM_FILTER	: {isKeyword(input, "BLOOM_FILTER")}? ID;
keyBLOCK_SIZE	: {isKeyword(input, "BLOCK_SIZE")}? ID;
keyIN_MEMORY	: {isKeyword(input, "IN_MEMORY")}? ID;
keySCANNER_CACHE_SIZE		: {isKeyword(input, "SCANNER_CACHE_SIZE")}? ID;
keyBLOCK_CACHE_ENABLED		: {isKeyword(input, "BLOCK_CACHE_ENABLED")}? ID;
keyCOMPRESSION_TYPE		: {isKeyword(input, "COMPRESSION_TYPE")}? ID;
keyMAP_FILE_INDEX_INTERVAL	: {isKeyword(input, "MAP_FILE_INDEX_INTERVAL")}? ID;
