package org.apache.hadoop.hbase.hbql.query.expr.value.expr;

import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.GenericExpr;

public abstract class GenericCalculation extends GenericExpr {

    private final Operator operator;

    protected GenericCalculation(final Type type,
                                 final GenericValue arg0,
                                 final Operator operator,
                                 final GenericValue arg1) {
        super(type, arg0, arg1);
        this.operator = operator;
    }

    protected Operator getOperator() {
        return this.operator;
    }

    public String asString() {
        if (this.getOperator() == Operator.NEGATIVE)
            return "-" + this.getArg(0).asString();
        else
            return this.getArg(0).asString() + " " + this.getOperator() + " " + this.getArg(1).asString();
    }
}
