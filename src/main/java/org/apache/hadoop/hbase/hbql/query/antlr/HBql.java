package org.apache.hadoop.hbase.hbql.query.antlr;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.Lexer;
import org.antlr.runtime.RecognitionException;
import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.antlr.args.QueryArgs;
import org.apache.hadoop.hbase.hbql.query.antlr.args.WhereArgs;
import org.apache.hadoop.hbase.hbql.query.antlr.cmds.ConnectionCmd;
import org.apache.hadoop.hbase.hbql.query.antlr.cmds.SchemaManagerCmd;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.schema.Schema;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 9:56:22 PM
 */
public class HBql {

    private static HBqlParser newParser(final String input) {
        System.out.println("Parsing: " + input);
        final Lexer lex = new HBqlLexer(new ANTLRStringStream(input));
        final CommonTokenStream tokens = new CommonTokenStream(lex);
        return new HBqlParser(tokens);
    }

    public static ExprTree parseDescWhereExpr(final String input,
                                              final Schema schema,
                                              final boolean optimize) throws HPersistException {
        try {
            final HBqlParser parser = newParser(input);
            final ExprTree exprTree = parser.descWhereExpr(schema);
            exprTree.setSchema(schema);
            exprTree.validateTypes();
            if (optimize)
                exprTree.optimize();
            return exprTree;
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HPersistException("Error parsing");
        }
    }

    public static ExprTree parseNoDescWhereExpr(final String input,
                                                final Schema schema,
                                                final boolean optimize) throws HPersistException {
        try {
            final HBqlParser parser = newParser(input);
            final ExprTree exprTree = parser.nodescWhereExpr(schema);
            exprTree.setSchema(schema);
            exprTree.validateTypes();
            if (optimize)
                exprTree.optimize();
            return exprTree;
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HPersistException("Error parsing");
        }
    }

    public static String parseStringValue(final String input) throws HPersistException {
        try {
            final HBqlParser parser = newParser(input);
            return parser.stringValue().getValue(null);
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HPersistException("Error parsing");
        }
    }

    public static Number parseNumberValue(final String input) throws HPersistException {
        try {
            final HBqlParser parser = newParser(input);
            return parser.numberValue().getValue(null);
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HPersistException("Error parsing");
        }
    }

    public static Long parseDateValue(final String input) throws HPersistException {
        try {
            final HBqlParser parser = newParser(input);
            return parser.dateValue().getValue(null);
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HPersistException("Error parsing");
        }
    }

    public static WhereArgs parseWithClause(final String input, final Schema schema) throws HPersistException {
        try {
            final HBqlParser parser = newParser(input);
            return parser.whereValue(schema);
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HPersistException("Error parsing");
        }
    }

    public static SchemaManagerCmd parseSchema(final String input) throws HPersistException {
        try {
            final HBqlParser parser = newParser(input);
            return parser.schemaExec();
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HPersistException("Error parsing");
        }
    }

    public static ConnectionCmd parseCommand(final String input) throws HPersistException {
        try {
            final HBqlParser parser = newParser(input);
            return parser.connectionExec();
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HPersistException("Error parsing");
        }
    }

    public static QueryArgs parseQuery(final String input, final Schema schema) throws HPersistException {
        try {
            final HBqlParser parser = newParser(input);
            return parser.selectStmt(schema);
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HPersistException("Error parsing");
        }
    }

}
