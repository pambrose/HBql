package org.apache.hadoop.hbase.hbql.query.antlr;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.Lexer;
import org.antlr.runtime.RecognitionException;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.antlr.args.QueryArgs;
import org.apache.hadoop.hbase.hbql.query.antlr.args.WhereArgs;
import org.apache.hadoop.hbase.hbql.query.antlr.cmds.ConnectionCmd;
import org.apache.hadoop.hbase.hbql.query.antlr.cmds.SchemaManagerCmd;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.schema.Schema;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 9:56:22 PM
 */
public class HBql {

    public static HBqlParser newParser(final String input) {
        System.out.println("Parsing: " + input);
        final Lexer lex = new HBqlLexer(new ANTLRStringStream(input));
        final CommonTokenStream tokens = new CommonTokenStream(lex);
        return new HBqlParser(tokens);
    }

    public static ExprTree parseWhereExpression(final String input,
                                                final Schema schema) throws HBqlException {
        try {
            final HBqlParser parser = newParser(input);
            final ExprTree exprTree = parser.nodescWhereExpr(schema);
            exprTree.setSchema(schema);
            return exprTree;
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HBqlException("Error parsing");
        }
    }

    public static String parseStringValue(final String input) throws HBqlException {
        try {
            final HBqlParser parser = newParser(input);
            final GenericValue valueExpr = parser.valueExpr();
            valueExpr.validateTypes(null, false);
            return (String)valueExpr.getValue(null);
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HBqlException("Error parsing");
        }
    }

    public static Number parseNumberValue(final String input) throws HBqlException {
        try {
            final HBqlParser parser = newParser(input);
            final GenericValue valueExpr = parser.valueExpr();
            valueExpr.validateTypes(null, false);
            return (Number)valueExpr.getValue(null);
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HBqlException("Error parsing");
        }
    }

    public static Long parseDateValue(final String input) throws HBqlException {
        try {
            final HBqlParser parser = newParser(input);
            final GenericValue valueExpr = parser.valueExpr();
            valueExpr.validateTypes(null, false);
            return (Long)valueExpr.getValue(null);
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HBqlException("Error parsing");
        }
    }

    public static WhereArgs parseWithClause(final String input, final Schema schema) throws HBqlException {
        try {
            final HBqlParser parser = newParser(input);
            return parser.whereValue(schema);
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HBqlException("Error parsing");
        }
    }

    public static SchemaManagerCmd parseSchema(final String input) throws HBqlException {
        try {
            final HBqlParser parser = newParser(input);
            return parser.schemaExec();
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HBqlException("Error parsing");
        }
    }

    public static ConnectionCmd parseCommand(final String input) throws HBqlException {
        try {
            final HBqlParser parser = newParser(input);
            return parser.connectionExec();
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HBqlException("Error parsing");
        }
    }

    public static QueryArgs parseQuery(final String input) throws HBqlException {
        try {
            final HBqlParser parser = newParser(input);
            return parser.selectStmt();
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HBqlException("Error parsing");
        }
    }

}
