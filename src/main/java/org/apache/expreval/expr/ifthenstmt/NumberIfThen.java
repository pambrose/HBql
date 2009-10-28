package org.apache.expreval.expr.ifthenstmt;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.NumberValue;

public class NumberIfThen extends GenericIfThen implements NumberValue {

    public NumberIfThen(final GenericValue arg0, final GenericValue arg1, final GenericValue arg2) {
        super(ExpressionType.NUMBERIFTHEN, arg0, arg1, arg2);
    }

    public Number getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Number)super.getValue(object);
    }
}
