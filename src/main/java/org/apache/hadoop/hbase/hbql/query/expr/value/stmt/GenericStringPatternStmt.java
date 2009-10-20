package org.apache.hadoop.hbase.hbql.query.expr.value.stmt;

import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

public abstract class GenericStringPatternStmt extends GenericNotValue {

    protected GenericStringPatternStmt(final GenericValue arg0, final boolean not, final GenericValue arg1) {
        super(Type.STRINGPATTERN, not, arg0, arg1);
    }

    protected abstract String getFunctionName();

    public String asString() {
        return this.getArg(0).asString() + notAsString() + " "
               + this.getFunctionName() + " " + this.getArg(1).asString();
    }
}