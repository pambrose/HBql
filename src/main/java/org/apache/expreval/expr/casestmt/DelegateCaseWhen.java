package org.apache.expreval.expr.casestmt;

import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.expreval.expr.node.StringValue;
import org.apache.expreval.util.HUtil;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.ResultMissingColumnException;

public class DelegateCaseWhen extends GenericCaseWhen {

    public DelegateCaseWhen(final GenericValue arg0, final GenericValue arg1) {
        super(null, arg0, arg1);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws HBqlException {

        this.validateParentClass(BooleanValue.class, this.getArg(0).validateTypes(this, false));
        final Class<? extends GenericValue> valueType = this.getArg(1).validateTypes(this, false);

        if (HUtil.isParentClass(StringValue.class, valueType))
            this.setTypedExpr(new StringCaseWhen(this.getArg(0), this.getArg(1)));
        else if (HUtil.isParentClass(NumberValue.class, valueType))
            this.setTypedExpr(new NumberCaseWhen(this.getArg(0), this.getArg(1)));
        else if (HUtil.isParentClass(DateValue.class, valueType))
            this.setTypedExpr(new DateCaseWhen(this.getArg(0), this.getArg(1)));
        else if (HUtil.isParentClass(BooleanValue.class, valueType))
            this.setTypedExpr(new BooleanCaseWhen(this.getArg(0), this.getArg(1)));
        else
            this.throwInvalidTypeException(valueType);

        return this.getTypedExpr().validateTypes(parentExpr, false);
    }

    public GenericValue getOptimizedValue() throws HBqlException {
        this.optimizeArgs();
        return this;
    }

    public Object getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return this.getTypedExpr().getValue(object);
    }
}