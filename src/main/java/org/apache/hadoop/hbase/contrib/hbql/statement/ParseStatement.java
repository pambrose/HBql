package org.apache.hadoop.hbase.contrib.hbql.statement;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.contrib.hbql.client.Output;

public class ParseStatement implements SchemaManagerStatement {

    private final ShellStatement stmt;
    private final GenericValue value;

    public ParseStatement(final ShellStatement stmt) {
        this.stmt = stmt;
        this.value = null;
    }

    public ParseStatement(final GenericValue value) {
        this.stmt = null;
        this.value = value;
    }

    private ShellStatement getStmt() {
        return this.stmt;
    }

    private GenericValue getValue() {
        return this.value;
    }

    public Output execute() throws HBqlException {
        final Output retval = new Output("Parsed successfully");
        if (this.getStmt() != null)
            retval.out.println(this.getStmt().getClass().getSimpleName());

        if (this.getValue() != null) {
            Object val = null;
            try {
                this.getValue().validateTypes(null, false);
                val = this.getValue().getValue(null);
            }
            catch (ResultMissingColumnException e) {
                val = "ResultMissingColumnException()";
            }
            retval.out.println(this.getValue().asString() + " = " + val);
        }

        return retval;
    }
}