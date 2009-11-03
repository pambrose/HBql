package org.apache.expreval.expr.calculation;

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.Operator;
import org.apache.expreval.expr.TypeSupport;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.expreval.expr.node.StringValue;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

public class DelegateCalculation extends GenericCalculation {

    public DelegateCalculation(final GenericValue arg0, final Operator operator, final GenericValue arg1) {
        super(null, arg0, operator, arg1);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws HBqlException {

        final Class<? extends GenericValue> type1 = this.getArg(0).validateTypes(this, false);
        final Class<? extends GenericValue> type2 = this.getArg(1).validateTypes(this, false);

        if (TypeSupport.isParentClass(StringValue.class, type1, type2))
            this.setTypedExpr(new StringCalculation(this.getArg(0), this.getOperator(), this.getArg(1)));
        else if (TypeSupport.isParentClass(NumberValue.class, type1, type2))
            this.setTypedExpr(new NumberCalculation(this.getArg(0), this.getOperator(), this.getArg(1)));
        else if (TypeSupport.isParentClass(DateValue.class, type1, type2))
            this.setTypedExpr(new DateCalculation(this.getArg(0), this.getOperator(), this.getArg(1)));
        else
            this.throwInvalidTypeException(type1, type2);

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