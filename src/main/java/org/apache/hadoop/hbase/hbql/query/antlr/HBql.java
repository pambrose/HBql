package org.apache.hadoop.hbase.hbql.query.antlr;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.Lexer;
import org.antlr.runtime.RecognitionException;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.query.cmds.ConnectionCmd;
import org.apache.hadoop.hbase.hbql.query.cmds.SchemaManagerCmd;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.schema.Schema;
import org.apache.hadoop.hbase.hbql.query.stmt.args.QueryArgs;
import org.apache.hadoop.hbase.hbql.query.stmt.args.WhereArgs;
import org.apache.hadoop.hbase.hbql.query.stmt.select.ExprSelectElement;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 9:56:22 PM
 */
public class HBql {

    public static HBqlParser newParser(final String str) {
        System.out.println("Parsing: " + str);
        final Lexer lex = new HBqlLexer(new ANTLRStringStream(str));
        final CommonTokenStream tokens = new CommonTokenStream(lex);
        return new HBqlParser(tokens);
    }

    public static ExprTree parseWhereExpression(final String str,
                                                final Schema schema) throws HBqlException {
        try {
            final HBqlParser parser = newParser(str);
            final ExprTree exprTree = parser.nodescWhereExpr();
            exprTree.setSchema(schema);
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
            final GenericValue valueExpr = parser.valExpr();
            valueExpr.validateTypes(null, false);
            return valueExpr.getValue(null);
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

    public static Object parseAndEvaluateSelectElement(final String str) throws HBqlException {
        final ExprSelectElement elem = parseSelectElement(str);
        return evaluateSelectElement(elem);
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
