package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.GenericExpr;

import java.util.List;

public abstract class GenericNotValue extends GenericExpr implements BooleanValue {

    private final boolean not;

    protected GenericNotValue(final Type type, final boolean not, GenericValue... args) {
        super(type, args);
        this.not = not;
    }

    protected GenericNotValue(final Type type, final boolean not, final List<GenericValue> args) {
        super(type, args);
        this.not = not;
    }

    protected GenericNotValue(final Type type, final boolean not, final GenericValue arg, final List<GenericValue> argList) {
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