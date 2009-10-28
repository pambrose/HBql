package org.apache.hadoop.hbase.hbql.stmt.expr;

import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;

import java.util.List;

public abstract class NotValue<T extends GenericExpression> extends DelegateStmt<T> {

    private final boolean not;

    protected NotValue(final ExpressionType type, final boolean not, GenericValue... args) {
        super(type, args);
        this.not = not;
    }

    protected NotValue(final ExpressionType type, final boolean not, final List<GenericValue> args) {
        super(type, args);
        this.not = not;
    }

    protected NotValue(final ExpressionType type, final boolean not, final GenericValue arg, final List<GenericValue> argList) {
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