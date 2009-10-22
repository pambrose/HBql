package org.apache.hadoop.hbase.hbql.query.expr.value.stmt;

import org.apache.hadoop.hbase.hbql.query.expr.GenericExpr;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

import java.util.List;

public abstract class NotValue<T extends GenericExpr> extends DelegateStmt<T> {

    private final boolean not;

    protected NotValue(final Type type, final boolean not, GenericValue... args) {
        super(type, args);
        this.not = not;
    }

    protected NotValue(final Type type, final boolean not, final List<GenericValue> args) {
        super(type, args);
        this.not = not;
    }

    protected NotValue(final Type type, final boolean not, final GenericValue arg, final List<GenericValue> argList) {
        super(type, arg, argList);
        this.not = not;
    }

    public boolean isNot() {
        return not;
    }

    protected String notAsString() {
        return (this.isNot()) ? " NOT" : "";
    }
}