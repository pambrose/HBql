package org.apache.hadoop.hbase.hbql.query.antlr;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.Lexer;
import org.antlr.runtime.RecognitionException;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.InternalErrorException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.cmds.ConnectionCmd;
import org.apache.hadoop.hbase.hbql.query.cmds.SchemaManagerCmd;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.schema.Schema;
import org.apache.hadoop.hbase.hbql.query.stmt.args.QueryArgs;
import org.apache.hadoop.hbase.hbql.query.stmt.args.WhereArgs;
import org.apache.hadoop.hbase.hbql.query.stmt.select.ExprSelectElement;

public class HBql {

    public static HBqlParser newParser(final String str) {
        System.out.println("Parsing: " + str);
        final Lexer lex = new HBqlLexer(new ANTLRStringStream(str));
        final CommonTokenStream tokens = new CommonTokenStream(lex);
        return new HBqlParser(tokens);
    }

    public static ExprTree parseWhereExpression(final String str, final Schema schema) throws HBqlException {
        try {

            // Fist see if schema already has tree cached
            ExprTree exprTree = schema.getExprTree(str);
            if (exprTree == null) {
                final HBqlParser parser = newParser(str);
                exprTree = parser.nodescWhereExpr();
                exprTree.setExprText(str);
                exprTree.setSchema(schema);
            }
            return exprTree;
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

    public static ExprSelectElement parseSelectElement(final String str) throws HBqlException {
        try {
            final HBqlParser parser = newParser(str);
            final ExprSelectElement elem = (ExprSelectElement)parser.selectElem();
            elem.setSchema(null);
            return elem;
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HBqlException("Error parsing: " + str);
        }
    }

    public static Object evaluateSelectElement(final ExprSelectElement elem) throws HBqlException {
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

    public static SchemaManagerCmd parseSchema(final String str) throws HBqlException {
        try {
            final HBqlParser parser = newParser(str);
            return parser.schemaStmt();
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HBqlException("Error parsing: " + str);
        }
    }

    public static ConnectionCmd parseCommand(final String str) throws HBqlException {
        try {
            final HBqlParser parser = newParser(str);
            return parser.commandStmt();
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HBqlException("Error parsing: " + str);
        }
    }

    public static QueryArgs parseSelectStmt(final HConnection connection, final String str) throws HBqlException {
        try {
            final HBqlParser parser = newParser(str);
            final QueryArgs args = parser.selectStmt();
            args.validate(connection);
            return args;
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HBqlException("Error parsing: " + str);
        }
    }
}
