package org.apache.hadoop.hbase.hbql.stmt.antlr;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.Lexer;
import org.antlr.runtime.RecognitionException;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.InternalErrorException;
import org.apache.hadoop.hbase.hbql.client.PreparedStatement;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.stmt.ConnectionStatement;
import org.apache.hadoop.hbase.hbql.stmt.SchemaManagerStatement;
import org.apache.hadoop.hbase.hbql.stmt.ShellStatement;
import org.apache.hadoop.hbase.hbql.stmt.args.WhereArgs;
import org.apache.hadoop.hbase.hbql.stmt.expr.ExpressionTree;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.stmt.schema.Schema;
import org.apache.hadoop.hbase.hbql.stmt.schema.SelectStatement;
import org.apache.hadoop.hbase.hbql.stmt.select.SingleExpression;

public class HBql {

    public static HBqlParser newParser(final String str) {
        System.out.println("Parsing: " + str);
        final Lexer lex = new HBqlLexer(new ANTLRStringStream(str));
        final CommonTokenStream tokens = new CommonTokenStream(lex);
        return new HBqlParser(tokens);
    }

    public static ExpressionTree parseWhereExpression(final String str, final Schema schema) throws HBqlException {
        try {
            return schema.getExprTree(str);
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HBqlException("Error parsing: " + str);
        }
    }

    public static Object parseExpression(final String str) throws HBqlException {
        try {
            final HBqlParser parser = newParser(str);
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
            throw new HBqlException("Error parsing: " + str);
        }
    }

    public static SingleExpression parseSelectElement(final String str) throws HBqlException {
        try {
            final HBqlParser parser = newParser(str);
            final SingleExpression elem = (SingleExpression)parser.selectElem();
            elem.setSchema(null);
            return elem;
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HBqlException("Error parsing: " + str);
        }
    }

    public static Object evaluateSelectElement(final SingleExpression elem) throws HBqlException {
        return elem.getValue(null);
    }

    public static WhereArgs parseWithClause(final String str) throws HBqlException {
        try {
            final HBqlParser parser = newParser(str);
            return parser.whereValue();
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HBqlException("Error parsing: " + str);
        }
    }

    private static ShellStatement parse(final String str) throws HBqlException {
        try {
            final HBqlParser parser = newParser(str);
            final ShellStatement stmt = parser.commandStmt();
            if (stmt == null)
                throw new HBqlException("Error parsing: " + str);
            return stmt;
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HBqlException("Error parsing: " + str);
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
            throw new HBqlException("Expecting an prepared statement");

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
