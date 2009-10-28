package org.apache.expreval.expr.casestmt;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.expreval.expr.node.StringValue;
import org.apache.expreval.util.HUtil;

public class DelegateCaseElse extends GenericCaseElse {

    public DelegateCaseElse(final GenericValue arg0) {
        super(null, arg0);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws HBqlException {

        final Class<? extends GenericValue> valueType = this.getArg(0).validateTypes(this, false);

        if (HUtil.isParentClass(StringValue.class, valueType))
            this.setTypedExpr(new StringCaseElse(this.getArg(0)));
        else if (HUtil.isParentClass(NumberValue.class, valueType))
            this.setTypedExpr(new NumberCaseElse(this.getArg(0)));
        else if (HUtil.isParentClass(DateValue.class, valueType))
            this.setTypedExpr(new DateCaseElse(this.getArg(0)));
        else if (HUtil.isParentClass(BooleanValue.class, valueType))
            this.setTypedExpr(new BooleanCaseElse(this.getArg(0)));
        else
            this.throwInvalidTypeException(valueType);

        return this.getTypedExpr().validateTypes(parentExpr, false);
    }

    public GenericValue getOptimizedValue() throws HBqlException {
        this.optimizeArgs();
        return !this.isAConstant() ? this : this.getTypedExpr().getOptimizedValue();
    }

    public Object getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return this.getTypedExpr().getValue(object);
    }
}