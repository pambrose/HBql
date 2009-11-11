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
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.ParseException;
import org.apache.hadoop.hbase.hbql.client.PreparedStatement;
import org.apache.hadoop.hbase.hbql.schema.Schema;
import org.apache.hadoop.hbase.hbql.statement.ConnectionStatement;
import org.apache.hadoop.hbase.hbql.statement.NonConnectionStatement;
import org.apache.hadoop.hbase.hbql.statement.SelectStatement;
import org.apache.hadoop.hbase.hbql.statement.ShellStatement;
import org.apache.hadoop.hbase.hbql.statement.args.WithArgs;
import org.apache.hadoop.hbase.hbql.statement.select.SingleExpressionContext;

import java.util.List;

public class HBqlShell {

    private static final Log log = LogFactory.getLog(HBqlShell.class.getName());

    public static HBqlParser newHBqlParser(final String str) throws ParseException {
        try {
            log.debug("Parsing: " + str);
            final Lexer lex = new HBqlLexer(new ANTLRStringStream(str));
            final CommonTokenStream tokens = new CommonTokenStream(lex);
            return new HBqlParser(tokens);
        }
        catch (LexerRecognitionException e) {
            throw new ParseException(e.getRecognitionExecption(), e.getMessage());
        }
    }

    public static ExpressionTree parseWhereExpression(final String str, final Schema schema) throws HBqlException {
        try {
            return schema.getExpressionTree(str);
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HBqlException("Error parsing: " + str);
        }
    }

    public static Object parseExpression(final String str) throws HBqlException {
        try {
            final HBqlParser parser = newHBqlParser(str);
            final GenericValue valueExpr = parser.topExpr();
            valueExpr.validateTypes(null, false);
            return valueExpr.getValue(null);
        }
        catch (ResultMissingColumnException e) {
            // No column refes to be missing
            throw new InternalErrorException(e.getMessage());
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new ParseException(e, str);
        }
    }

    public static SingleExpressionContext parseSelectElement(final String str) throws HBqlException {
        try {
            final HBqlParser parser = newHBqlParser(str);
            final SingleExpressionContext elem = (SingleExpressionContext)parser.selectElem();
            elem.setSchemaAndContext(null);
            return elem;
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new ParseException(e, str);
        }
    }

    public static Object evaluateSelectElement(final SingleExpressionContext elem) throws HBqlException {
        return elem.getValue(null);
    }

    public static WithArgs parseWithClause(final String str) throws ParseException {
        try {
            final HBqlParser parser = newHBqlParser(str);
            return parser.withClause();
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new ParseException(e, str);
        }
    }

    public static List<ShellStatement> parseCommands(final String str) throws ParseException {
        try {
            final HBqlParser parser = newHBqlParser(str);
            return parser.shellCommand();
        }
        catch (LexerRecognitionException e) {
            // e.printStackTrace();
            throw new ParseException(e.getRecognitionExecption(), str);
        }
        catch (RecognitionException e) {
            // e.printStackTrace();
            throw new ParseException(e, str);
        }
    }

    private static ShellStatement parse(final String str) throws ParseException {
        try {
            final HBqlParser parser = newHBqlParser(str);
            return parser.commandStmt();
        }
        catch (RecognitionException e) {
            //e.printStackTrace();
            throw new ParseException(e, str);
        }
    }

    public static NonConnectionStatement parseSchemaManagerStatement(final String str) throws HBqlException {
        final ShellStatement statement = parse(str);

        if (!(statement instanceof NonConnectionStatement))
            throw new HBqlException("Expecting a schema manager statement");

        return (NonConnectionStatement)statement;
    }

    public static ConnectionStatement parseConnectionStatement(final String str) throws HBqlException {

        final ShellStatement statement = parse(str);

        if (!(statement instanceof ConnectionStatement))
            throw new HBqlException("Expecting a connection statement");

        return (ConnectionStatement)statement;
    }

    public static PreparedStatement parsePreparedStatement(final String str) throws HBqlException {

        final ShellStatement statement = parse(str);

        if (!(statement instanceof PreparedStatement))
            throw new HBqlException("Expecting a prepared statement");

        return (PreparedStatement)statement;
    }

    public static SelectStatement parseSelectStatement(final HConnection connection, final String str) throws HBqlException {

        final ShellStatement statement = parse(str);

        if (!(statement instanceof SelectStatement))
            throw new HBqlException("Expecting a select statement");

        final SelectStatement selectStatement = (SelectStatement)statement;

        selectStatement.validate(connection);

        return selectStatement;
    }
}
