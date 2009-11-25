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

package org.apache.hadoop.hbase.hbql.parser;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.Lexer;
import org.antlr.runtime.RecognitionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.expreval.client.InternalErrorException;
import org.apache.expreval.client.LexerRecognitionException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionTree;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.antlr.HBqlLexer;
import org.apache.hadoop.hbase.hbql.antlr.HBqlParser;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ParseException;
import org.apache.hadoop.hbase.hbql.statement.HBqlStatement;
import org.apache.hadoop.hbase.hbql.statement.StatementContext;
import org.apache.hadoop.hbase.hbql.statement.args.WithArgs;
import org.apache.hadoop.hbase.hbql.statement.select.SelectExpressionContext;

import java.util.List;

public class ParserUtil {

    private static final Log log = LogFactory.getLog(ParserUtil.class.getName());

    public static HBqlParser newHBqlParser(final String sql) throws ParseException {
        try {
            log.debug("Parsing: " + sql);
            final Lexer lex = new HBqlLexer(new ANTLRStringStream(sql));
            final CommonTokenStream tokens = new CommonTokenStream(lex);
            return new HBqlParser(tokens);
        }
        catch (LexerRecognitionException e) {
            throw new ParseException(e.getRecognitionExecption(), e.getMessage());
        }
    }

    public static ExpressionTree parseWhereExpression(final String sql,
                                                      final StatementContext statementContext) throws HBqlException {
        try {
            return statementContext.getMapping().getExpressionTree(sql, statementContext);
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HBqlException("Error parsing: " + sql);
        }
    }

    public static Object parseExpression(final String sql) throws HBqlException {
        try {
            final HBqlParser parser = ParserUtil.newHBqlParser(sql);
            final GenericValue valueExpr = parser.exprValue();
            valueExpr.validateTypes(null, false);
            return valueExpr.getValue(null);
        }
        catch (ResultMissingColumnException e) {
            // No column refs should be missing
            throw new InternalErrorException(e.getMessage());
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new ParseException(e, sql);
        }
    }

    public static SelectExpressionContext parseSelectElement(final String sql) throws HBqlException {
        try {
            final HBqlParser parser = ParserUtil.newHBqlParser(sql);
            final SelectExpressionContext elem = (SelectExpressionContext)parser.selectElem();
            elem.setStatementContext(null);
            return elem;
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new ParseException(e, sql);
        }
    }

    public static Object evaluateSelectElement(final SelectExpressionContext elem) throws HBqlException {
        return elem.getValue(null);
    }

    public static WithArgs parseWithClause(final String sql) throws ParseException {
        try {
            final HBqlParser parser = ParserUtil.newHBqlParser(sql);
            return parser.withClause();
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new ParseException(e, sql);
        }
    }

    public static List<HBqlStatement> parseConsoleStatements(final String sql) throws HBqlException {
        try {
            final HBqlParser parser = ParserUtil.newHBqlParser(sql);
            final List<HBqlStatement> stmts = parser.consoleStatements();
            for (final HBqlStatement stmt : stmts)
                stmt.validate();
            return stmts;
        }
        catch (LexerRecognitionException e) {
            throw new ParseException(e.getRecognitionExecption(), sql);
        }
        catch (RecognitionException e) {
            throw new ParseException(e, sql);
        }
    }

    public static HBqlStatement parseJdbcStatement(final String sql) throws HBqlException {
        try {
            final HBqlParser parser = ParserUtil.newHBqlParser(sql);
            final HBqlStatement stmt = parser.jdbcStatement();
            stmt.validate();
            return stmt;
        }
        catch (LexerRecognitionException e) {
            throw new ParseException(e.getRecognitionExecption(), sql);
        }
        catch (RecognitionException e) {
            throw new ParseException(e, sql);
        }
    }
}
