package org.apache.hadoop.hbase.hbql.query.expr.stringpattern;

import org.apache.hadoop.hbase.hbql.query.expr.NotValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

public abstract class GenericStringPatternStmt extends NotValue<GenericStringPatternStmt> implements BooleanValue {

    protected GenericStringPatternStmt(final GenericValue arg0, final boolean not, final GenericValue arg1) {
        super(Type.STRINGPATTERN, not, arg0, arg1);
    }

    protected abstract String getFunctionName();

    public String asString() {
        return this.getArg(0).asString() + notAsString() + " "
               + this.getFunctionName() + " " + this.getArg(1).asString();
    }
}