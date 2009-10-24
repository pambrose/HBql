package org.apache.hadoop.hbase.hbql.query.antlr;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.Lexer;
import org.antlr.runtime.RecognitionException;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.InternalErrorException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.cmds.ConnectionCommand;
import org.apache.hadoop.hbase.hbql.query.cmds.SchemaManagerCommand;
import org.apache.hadoop.hbase.hbql.query.cmds.ShellCommand;
import org.apache.hadoop.hbase.hbql.query.cmds.schema.SelectRecords;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.schema.Schema;
import org.apache.hadoop.hbase.hbql.query.stmt.args.SelectStmt;
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

    private static ShellCommand parse(final String str) throws HBqlException {
        try {
            final HBqlParser parser = newParser(str);
            return parser.commandStmt();
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HBqlException("Error parsing: " + str);
        }
    }

    public static SchemaManagerCommand parseSchemaCommand(final String str) throws HBqlException {
        final ShellCommand command = parse(str);
        if (command instanceof SchemaManagerCommand)
            return (SchemaManagerCommand)command;
        else
            throw new HBqlException("Expecting a schema manager command");
    }

    public static ConnectionCommand parseCommand(final String str) throws HBqlException {
        final ShellCommand command = parse(str);
        if (command instanceof ConnectionCommand)
            return (ConnectionCommand)command;
        else
            throw new HBqlException("Expecting a connection command");
    }

    public static SelectStmt parseSelectStmt(final HConnection connection, final String str) throws HBqlException {
        final ShellCommand command = parse(str);
        if (command instanceof SelectRecords) {
            final SelectStmt args = ((SelectRecords)command).getQueryArgs();
            args.validate(connection);
            return args;
        }
        else {
            throw new HBqlException("Expecting a select stmt");
        }
    }
}
