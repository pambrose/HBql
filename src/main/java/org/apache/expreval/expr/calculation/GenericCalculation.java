package org.apache.expreval.expr.calculation;

import org.apache.expreval.expr.DelegateStmt;
import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.Operator;
import org.apache.expreval.expr.node.GenericValue;

public abstract class GenericCalculation extends DelegateStmt<GenericCalculation> {

    private final Operator operator;

    protected GenericCalculation(final ExpressionType type,
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
