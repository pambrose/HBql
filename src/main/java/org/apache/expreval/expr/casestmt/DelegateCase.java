package org.apache.expreval.expr.casestmt;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.Util;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.expreval.expr.node.StringValue;

import java.util.ArrayList;

public class DelegateCase extends GenericCase {

    public DelegateCase() {
        super(null, new ArrayList<GenericCaseWhen>(), null);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws HBqlException {

        final Class<? extends GenericValue> type = this.getWhenExprList().get(0).validateTypes(this, false);
        final Class<? extends GenericValue> argType = this.determineGenericValueClass(type);

        for (final GenericCaseWhen val : this.getWhenExprList())
            this.validateParentClass(argType, val.validateTypes(this, false));

        if (this.getElseExpr() != null)
            this.validateParentClass(argType, this.getElseExpr().validateTypes(parentExpr, false));

        if (Util.isParentClass(StringValue.class, argType))
            this.setTypedExpr(new StringCase(this.getWhenExprList(), this.getElseExpr()));
        else if (Util.isParentClass(NumberValue.class, argType))
            this.setTypedExpr(new NumberCase(this.getWhenExprList(), this.getElseExpr()));
        else if (Util.isParentClass(DateValue.class, argType))
            this.setTypedExpr(new DateCase(this.getWhenExprList(), this.getElseExpr()));
        else if (Util.isParentClass(BooleanValue.class, argType))
            this.setTypedExpr(new BooleanCase(this.getWhenExprList(), this.getElseExpr()));
        else
            this.throwInvalidTypeException(argType);

        return this.getTypedExpr().validateTypes(parentExpr, false);
    }

    public GenericValue getOptimizedValue() throws HBqlException {
        this.optimizeArgs();
        return !this.isAConstant() ? this : this.getTypedExpr().getOptimizedValue();
    }

    public Object getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return this.getTypedExpr().getValue(object);
    }

    public void addWhen(final GenericValue pred, final GenericValue value) {
        this.getWhenExprList().add(new DelegateCaseWhen(pred, value));
    }

    public void addElse(final GenericValue value) {
        if (value != null)
            this.setElseExpr(new DelegateCaseElse(value));
    }
}