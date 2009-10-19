package org.apache.hadoop.hbase.hbql.query.expr.value.stmt;

import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

public abstract class GenericBetweenStmt extends GenericNotValue {

    protected GenericBetweenStmt(final Type type,
                                 final boolean not,
                                 final GenericValue arg0,
                                 final GenericValue arg1,
                                 final GenericValue arg2) {
        super(type, not, arg0, arg1, arg2);
    }

    public String asString() {
        return this.getArg(0).asString() + notAsString() + " BETWEEN "
               + this.getArg(1).asString() + " AND " + this.getArg(2).asString();
    }
}
