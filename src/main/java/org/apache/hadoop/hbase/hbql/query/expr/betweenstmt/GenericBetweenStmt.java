package org.apache.hadoop.hbase.hbql.query.expr.betweenstmt;

import org.apache.hadoop.hbase.hbql.query.expr.NotValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

public abstract class GenericBetweenStmt extends NotValue<GenericBetweenStmt> implements BooleanValue {

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
