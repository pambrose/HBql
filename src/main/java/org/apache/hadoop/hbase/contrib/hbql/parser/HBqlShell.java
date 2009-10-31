package org.apache.hadoop.hbase.contrib.hbql.parser;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.Lexer;
import org.antlr.runtime.RecognitionException;
import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.InternalErrorException;
import org.apache.expreval.client.ParseException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionTree;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.contrib.hbql.antlr.HBqlLexer;
import org.apache.hadoop.hbase.contrib.hbql.antlr.HBqlParser;
import org.apache.hadoop.hbase.contrib.hbql.client.Connection;
import org.apache.hadoop.hbase.contrib.hbql.client.PreparedStatement;
import org.apache.hadoop.hbase.contrib.hbql.schema.Schema;
import org.apache.hadoop.hbase.contrib.hbql.statement.ConnectionStatement;
import org.apache.hadoop.hbase.contrib.hbql.statement.SchemaManagerStatement;
import org.apache.hadoop.hbase.contrib.hbql.statement.SelectStatement;
import org.apache.hadoop.hbase.contrib.hbql.statement.ShellStatement;
import org.apache.hadoop.hbase.contrib.hbql.statement.args.WithArgs;
import org.apache.hadoop.hbase.contrib.hbql.statement.select.SingleExpressionContext;
import org.apache.log4j.Logger;

import java.util.List;

public class HBqlShell {

    final static Logger log = Logger.getLogger(HBqlShell.class.getSimpleName());

    public static HBqlParser newHBqlParser(final String str) {
        log.info("Parsing: " + str);
        final Lexer lex = new HBqlLexer(new ANTLRStringStream(str));
        final CommonTokenStream tokens = new CommonTokenStream(lex);
        return new HBqlParser(tokens);
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
            elem.setSchema(null);
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

    public static WithArgs parseWithClause(final String str) throws HBqlException {
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
        catch (RecognitionException e) {
            //e.printStackTrace();
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

    public static SchemaManagerStatement parseSchemaManagerStatement(final String str) throws HBqlException {
        final ShellStatement statement = parse(str);

        if (!(statement instanceof SchemaManagerStatement))
            throw new HBqlException("Expecting a schema manager statement");

        return (SchemaManagerStatement)statement;
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

    public static SelectStatement parseSelectStatement(final Connection connection, final String str) throws HBqlException {

        final ShellStatement statement = parse(str);

        if (!(statement instanceof SelectStatement))
            throw new HBqlException("Expecting a select statement");

        final SelectStatement selectStatement = (SelectStatement)statement;

        selectStatement.validate(connection);

        return selectStatement;
    }
}
